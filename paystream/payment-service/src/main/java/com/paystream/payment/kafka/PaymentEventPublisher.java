package com.paystream.payment.kafka;

import com.paystream.payment.model.PaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventPublisher {

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;
    public static final String PAYMENT_INITIATED_TOPIC = "payment.initiated";

    public void publishPaymentInitiated(PaymentEvent event) {
        kafkaTemplate.send(PAYMENT_INITIATED_TOPIC, event.getTransactionId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish payment event for txn={}", event.getTransactionId(), ex);
                    } else {
                        log.info("Published payment.initiated for txn={}", event.getTransactionId());
                    }
                });
    }
}
