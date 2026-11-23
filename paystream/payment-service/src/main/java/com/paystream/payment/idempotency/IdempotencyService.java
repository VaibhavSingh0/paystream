package com.paystream.payment.idempotency;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import java.time.Duration;

@Component
@RequiredArgsConstructor
public class IdempotencyService {

    private final StringRedisTemplate redisTemplate;
    private static final String KEY_PREFIX = "idempotency:";
    private static final Duration TTL = Duration.ofHours(24);

    public boolean isDuplicate(String transactionId) {
        String key = KEY_PREFIX + transactionId;
        Boolean wasAbsent = redisTemplate.opsForValue().setIfAbsent(key, "processed", TTL);
        return Boolean.FALSE.equals(wasAbsent);
    }
}
