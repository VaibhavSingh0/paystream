package com.paystream.fraud.consumer;

import com.paystream.fraud.model.FraudResultEvent;
import com.paystream.fraud.model.PaymentEvent;
import com.paystream.fraud.publisher.FraudResultPublisher;
import com.paystream.fraud.rules.FraudRule;
import com.paystream.fraud.rules.RuleResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class FraudCheckConsumer {

    private final List<FraudRule> fraudRules;
    private final FraudResultPublisher resultPublisher;

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2),
            dltTopicSuffix = ".dlq"
    )
    @KafkaListener(topics = "payment.initiated", groupId = "fraud-detection-group")
    public void consume(PaymentEvent event) {
        log.info("Received payment.initiated for txn={}", event.getTransactionId());

        double maxScore = 0.0;
        String fraudReason = null;
        boolean isFraud = false;

        for (FraudRule rule : fraudRules) {
            RuleResult result = rule.evaluate(event);
            if (result.isFraudulent() && result.getScore() > maxScore) {
                maxScore = result.getScore();
                fraudReason = result.getReason();
                isFraud = true;
            }
        }

        FraudResultEvent fraudResult = new FraudResultEvent();
        fraudResult.setTransactionId(event.getTransactionId());
        fraudResult.setMerchantId(event.getMerchantId());
        fraudResult.setCustomerId(event.getCustomerId());
        fraudResult.setAmount(event.getAmount());
        fraudResult.setFraudulent(isFraud);
        fraudResult.setReason(fraudReason);
        fraudResult.setFraudScore(maxScore);
        fraudResult.setTimestamp(LocalDateTime.now());

        resultPublisher.publishResult(fraudResult);
        log.info("Fraud check completed txn={} fraudulent={} score={}", event.getTransactionId(), isFraud, maxScore);
    }
}
