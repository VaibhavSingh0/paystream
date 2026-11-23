package com.paystream.payment.controller;

import com.paystream.payment.model.PaymentRequest;
import com.paystream.payment.model.Transaction;
import com.paystream.payment.service.PaymentService;
import com.paystream.payment.service.RateLimitExceededException;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping
    public ResponseEntity<Transaction> initiatePayment(
            @RequestHeader(value = "X-Transaction-Id", required = false) String transactionId,
            @RequestBody PaymentRequest request) {

        if (transactionId == null || transactionId.isBlank()) {
            transactionId = UUID.randomUUID().toString();
        }

        try {
            Transaction transaction = paymentService.initiatePayment(transactionId, request);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(transaction);
        } catch (RateLimitExceededException e) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build();
        }
    }

    @GetMapping("/{transactionId}")
    public ResponseEntity<Transaction> getTransaction(@PathVariable String transactionId) {
        return ResponseEntity.ok(paymentService.getTransaction(transactionId));
    }
}
