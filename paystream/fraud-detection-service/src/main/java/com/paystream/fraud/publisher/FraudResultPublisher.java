package com.paystream.fraud.publisher;

import com.paystream.fraud.model.FraudResultEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class FraudResultPublisher {

    private final KafkaTemplate<String, FraudResultEvent> kafkaTemplate;
    public static final String PAYMENT_APPROVED_TOPIC = "payment.approved";
    public static final String PAYMENT_FRAUD_FLAGGED_TOPIC = "payment.fraud-flagged";

    public void publishResult(FraudResultEvent result) {
        String topic = result.isFraudulent() ? PAYMENT_FRAUD_FLAGGED_TOPIC : PAYMENT_APPROVED_TOPIC;
        kafkaTemplate.send(topic, result.getTransactionId(), result)
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish fraud result for txn={}", result.getTransactionId(), ex);
                    } else {
                        log.info("Published {} for txn={}", topic, result.getTransactionId());
                    }
                });
    }
}
