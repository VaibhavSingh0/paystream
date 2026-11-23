package com.paystream.payment.model;

public enum TransactionStatus {
    INITIATED,
    FRAUD_CHECK_PENDING,
    APPROVED,
    FRAUD_FLAGGED,
    FAILED
}
