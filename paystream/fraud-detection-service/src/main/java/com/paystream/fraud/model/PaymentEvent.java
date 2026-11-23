package com.paystream.fraud.model;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class PaymentEvent {
    private String transactionId;
    private String merchantId;
    private String customerId;
    private BigDecimal amount;
    private String currency;
    private String sourceIp;
    private String eventType;
    private LocalDateTime timestamp;
}
