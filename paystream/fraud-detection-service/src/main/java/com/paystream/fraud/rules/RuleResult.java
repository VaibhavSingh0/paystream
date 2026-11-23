package com.paystream.fraud.rules;

import lombok.Getter;

@Getter
public class RuleResult {
    private final boolean fraudulent;
    private final String reason;
    private final double score;

    private RuleResult(boolean fraudulent, String reason, double score) {
        this.fraudulent = fraudulent;
        this.reason = reason;
        this.score = score;
    }

    public static RuleResult fraud(String reason, double score) {
        return new RuleResult(true, reason, score);
    }

    public static RuleResult pass() {
        return new RuleResult(false, null, 0.0);
    }
}
