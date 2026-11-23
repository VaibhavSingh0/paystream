package com.paystream.gateway.controller;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/api/v1/gateway/payments")
@RequiredArgsConstructor
@Slf4j
public class GatewayController {

    private final RestTemplate restTemplate;

    @Value("${payment.service.url:http://localhost:8081}")
    private String paymentServiceUrl;

    @CircuitBreaker(name = "payment-service", fallbackMethod = "paymentServiceFallback")
    @PostMapping
    public ResponseEntity<String> initiatePayment(
            @RequestHeader(value = "X-Transaction-Id", required = false) String transactionId,
            @RequestBody String body) {

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (transactionId != null) {
            headers.set("X-Transaction-Id", transactionId);
        }

        HttpEntity<String> entity = new HttpEntity<>(body, headers);
        return restTemplate.postForEntity(paymentServiceUrl + "/api/v1/payments", entity, String.class);
    }

    @CircuitBreaker(name = "payment-service", fallbackMethod = "getStatusFallback")
    @GetMapping("/{transactionId}/status")
    public ResponseEntity<String> getPaymentStatus(@PathVariable String transactionId) {
        return restTemplate.getForEntity(
                paymentServiceUrl + "/api/v1/payments/" + transactionId, String.class);
    }

    public ResponseEntity<String> paymentServiceFallback(Exception e) {
        log.error("Payment service circuit breaker open: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("{\"error\": \"Payment service temporarily unavailable. Please retry.\"}");
    }

    public ResponseEntity<String> getStatusFallback(String transactionId, Exception e) {
        log.error("Payment service circuit breaker open for status check: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("{\"error\": \"Status check unavailable. Please retry.\"}");
    }
}
