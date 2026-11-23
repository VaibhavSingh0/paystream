package com.paystream.notification.service;

import com.paystream.notification.model.NotificationLog;
import com.paystream.notification.model.PaymentResultEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;

@Service
@Slf4j
public class NotificationService {

    private final MongoTemplate mongoTemplate;

    public NotificationService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public void sendNotification(PaymentResultEvent event) {
        String message = buildMessage(event);
        log.info("[NOTIFICATION] txn={} fraudulent={} message={}",
                event.getTransactionId(), event.isFraudulent(), message);

        NotificationLog logEntry = new NotificationLog();
        logEntry.setTransactionId(event.getTransactionId());
        logEntry.setCustomerId(event.getCustomerId());
        logEntry.setMerchantId(event.getMerchantId());
        logEntry.setType(event.isFraudulent() ? "FRAUD_ALERT" : "PAYMENT_APPROVED");
        logEntry.setStatus("SENT");
        logEntry.setMessage(message);
        logEntry.setSentAt(LocalDateTime.now());
        mongoTemplate.save(logEntry);
    }

    private String buildMessage(PaymentResultEvent event) {
        if (event.isFraudulent()) {
            return String.format("FRAUD ALERT: Transaction %s for amount %s was flagged. Reason: %s",
                    event.getTransactionId(), event.getAmount(), event.getReason());
        }
        return String.format("Payment Approved: Transaction %s for amount %s has been successfully processed.",
                event.getTransactionId(), event.getAmount());
    }
}
