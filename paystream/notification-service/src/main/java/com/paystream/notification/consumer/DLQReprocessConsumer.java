package com.paystream.notification.consumer;

import com.paystream.notification.model.PaymentResultEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DLQReprocessConsumer {

    @KafkaListener(topics = "payment.approved.dlq", groupId = "notification-dlq-group")
    public void handleApprovedDlq(PaymentResultEvent event) {
        log.error("DLQ: Failed to process payment.approved for txn={} after all retries", event.getTransactionId());
        // Persist to dead letter store or trigger alert
    }

    @KafkaListener(topics = "payment.fraud-flagged.dlq", groupId = "notification-dlq-group")
    public void handleFraudDlq(PaymentResultEvent event) {
        log.error("DLQ: Failed to process payment.fraud-flagged for txn={} after all retries", event.getTransactionId());
    }
}
