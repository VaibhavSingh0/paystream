package com.paystream.payment.service;

import com.paystream.payment.idempotency.IdempotencyService;
import com.paystream.payment.kafka.PaymentEventPublisher;
import com.paystream.payment.model.*;
import com.paystream.payment.ratelimit.RateLimitService;
import com.paystream.payment.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private final TransactionRepository transactionRepository;
    private final PaymentEventPublisher eventPublisher;
    private final IdempotencyService idempotencyService;
    private final RateLimitService rateLimitService;

    @Transactional
    public Transaction initiatePayment(String transactionId, PaymentRequest request) {
        // Idempotency check
        if (idempotencyService.isDuplicate(transactionId)) {
            log.warn("Duplicate request for transactionId={}", transactionId);
            return transactionRepository.findByTransactionId(transactionId)
                    .orElseThrow(() -> new IllegalStateException("Duplicate key but no record found"));
        }

        // Rate limit check
        if (!rateLimitService.isAllowed(request.getMerchantId())) {
            throw new RateLimitExceededException("Rate limit exceeded for merchant: " + request.getMerchantId());
        }

        // Persist transaction
        Transaction transaction = new Transaction();
        transaction.setTransactionId(transactionId);
        transaction.setMerchantId(request.getMerchantId());
        transaction.setCustomerId(request.getCustomerId());
        transaction.setAmount(request.getAmount());
        transaction.setCurrency(request.getCurrency());
        transaction.setSourceIp(request.getSourceIp());
        transaction.setStatus(TransactionStatus.INITIATED);
        transactionRepository.save(transaction);

        // Publish to Kafka
        PaymentEvent event = new PaymentEvent(
                transactionId,
                request.getMerchantId(),
                request.getCustomerId(),
                request.getAmount(),
                request.getCurrency(),
                request.getSourceIp(),
                "PAYMENT_INITIATED",
                LocalDateTime.now()
        );
        eventPublisher.publishPaymentInitiated(event);

        log.info("Payment initiated transactionId={} merchantId={} amount={}",
                transactionId, request.getMerchantId(), request.getAmount());
        return transaction;
    }

    public Transaction getTransaction(String transactionId) {
        return transactionRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new IllegalArgumentException("Transaction not found: " + transactionId));
    }

    @Transactional
    public void updateTransactionStatus(String transactionId, TransactionStatus status) {
        transactionRepository.findByTransactionId(transactionId).ifPresent(tx -> {
            tx.setStatus(status);
            transactionRepository.save(tx);
            log.info("Updated transactionId={} status={}", transactionId, status);
        });
    }
}
