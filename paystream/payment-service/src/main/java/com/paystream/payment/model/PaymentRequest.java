package com.paystream.payment.model;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class PaymentRequest {
    private String merchantId;
    private String customerId;
    private BigDecimal amount;
    private String currency;
    private String sourceIp;
}
