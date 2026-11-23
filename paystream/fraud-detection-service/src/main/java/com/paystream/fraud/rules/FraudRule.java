package com.paystream.fraud.rules;

import com.paystream.fraud.model.PaymentEvent;

public interface FraudRule {
    RuleResult evaluate(PaymentEvent event);
}
