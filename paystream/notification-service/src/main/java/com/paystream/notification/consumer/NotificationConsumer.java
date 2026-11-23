package com.paystream.notification.consumer;

import com.paystream.notification.model.PaymentResultEvent;
import com.paystream.notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumer {

    private final NotificationService notificationService;

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 2000, multiplier = 2),
            dltTopicSuffix = ".dlq"
    )
    @KafkaListener(topics = {"payment.approved", "payment.fraud-flagged"}, groupId = "notification-group")
    public void consume(PaymentResultEvent event) {
        log.info("Received notification trigger for txn={} fraudulent={}", event.getTransactionId(), event.isFraudulent());
        notificationService.sendNotification(event);
    }
}
