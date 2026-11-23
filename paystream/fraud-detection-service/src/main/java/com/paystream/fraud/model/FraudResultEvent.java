package com.paystream.fraud.model;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class FraudResultEvent {
    private String transactionId;
    private String merchantId;
    private String customerId;
    private BigDecimal amount;
    private boolean fraudulent;
    private String reason;
    private double fraudScore;
    private LocalDateTime timestamp;
}
