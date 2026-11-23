package com.paystream.fraud.rules;

import com.paystream.fraud.model.PaymentEvent;
import org.springframework.stereotype.Component;
import java.math.BigDecimal;

@Component
public class AmountAnomalyRule implements FraudRule {

    private static final BigDecimal HIGH_VALUE_THRESHOLD = new BigDecimal("100000");

    @Override
    public RuleResult evaluate(PaymentEvent event) {
        if (event.getAmount().compareTo(HIGH_VALUE_THRESHOLD) > 0) {
            return RuleResult.fraud(
                    "High-value transaction: " + event.getAmount() + " exceeds threshold",
                    0.6
            );
        }
        return RuleResult.pass();
    }
}
