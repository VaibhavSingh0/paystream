package com.paystream.notification.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

@Document(collection = "notification_logs")
@Data
public class NotificationLog {

    @Id
    private String id;
    private String transactionId;
    private String customerId;
    private String merchantId;
    private String type;
    private String status;
    private String message;
    private LocalDateTime sentAt;
}
