package com.paystream.fraud.rules;

import com.paystream.fraud.model.PaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import java.time.Duration;

@Component
@RequiredArgsConstructor
@Slf4j
public class VelocityRule implements FraudRule {

    private final StringRedisTemplate redisTemplate;
    private static final int MAX_TRANSACTIONS_PER_MINUTE = 5;

    @Override
    public RuleResult evaluate(PaymentEvent event) {
        String key = "velocity:" + event.getSourceIp();
        Long count = redisTemplate.opsForValue().increment(key);

        if (count == 1) {
            redisTemplate.expire(key, Duration.ofMinutes(1));
        }

        if (count > MAX_TRANSACTIONS_PER_MINUTE) {
            log.warn("Velocity breach: ip={} count={}", event.getSourceIp(), count);
            return RuleResult.fraud("Velocity limit exceeded: " + count + " txns/min from IP " + event.getSourceIp(), 0.8);
        }
        return RuleResult.pass();
    }
}
