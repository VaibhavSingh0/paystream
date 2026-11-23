# PayStream — High Level Design

## Overview
PayStream is a distributed, event-driven payment processing platform with real-time fraud detection.
It is designed to handle high-throughput financial transactions reliably and safely.

## Architecture

```
                        ┌─────────────────────────────────┐
                        │     Client (Mobile / Web App)    │
                        └────────────────┬────────────────┘
                                         │ HTTPS + JWT
                        ┌────────────────▼────────────────┐
                        │     payment-gateway-service      │
                        │  - JWT Authentication            │
                        │  - Circuit Breaker (Resilience4j)│
                        │  - Request routing               │
                        └────────────────┬────────────────┘
                                         │ REST
                        ┌────────────────▼────────────────┐
                        │        payment-service           │
                        │  - Idempotency (Redis + UUID)    │
                        │  - Rate Limiting (Bucket4j)      │
                        │  - Persist to MySQL              │
                        │  - Publish → Kafka               │
                        └────────────────┬────────────────┘
                                         │ Kafka: payment.initiated
                        ┌────────────────▼────────────────┐
                        │    fraud-detection-service       │
                        │  - Velocity Rule (Redis TTL)     │
                        │  - Amount Anomaly Rule           │
                        │  - Publishes result to Kafka     │
                        └────────┬───────────────┬────────┘
                  payment.approved               payment.fraud-flagged
                        │                               │
                        └───────────────┬───────────────┘
                                        │
                        ┌───────────────▼───────────────┐
                        │       notification-service     │
                        │  - Consumes both topics        │
                        │  - Sends mock SMS/Email        │
                        │  - DLQ with 3x retry + backoff │
                        │  - Logs to MongoDB             │
                        └───────────────────────────────┘

All services → Prometheus → Grafana
```

## Kafka Topics

| Topic                    | Producer               | Consumer                   |
|--------------------------|------------------------|----------------------------|
| payment.initiated        | payment-service        | fraud-detection-service    |
| payment.approved         | fraud-detection-service| notification-service       |
| payment.fraud-flagged    | fraud-detection-service| notification-service       |
| payment.approved.dlq     | Kafka (auto)           | DLQReprocessConsumer       |
| payment.fraud-flagged.dlq| Kafka (auto)           | DLQReprocessConsumer       |

## Key Design Decisions

### Why Kafka over REST for inter-service communication?
- Decouples producers and consumers — fraud service can be down without affecting payment intake
- Enables replay for auditing and reprocessing
- Natural backpressure via consumer group lag

### Why Choreography Saga over Orchestration?
- No single point of failure (no orchestrator service)
- Each service owns its own state transition
- Simpler to scale independently

### Why Redis for idempotency?
- O(1) set-if-absent with TTL — perfect for 24-hour dedup window
- Survives multiple payment-service instances (shared state)

### Why Bucket4j for rate limiting?
- In-process, no external dependency for rate limiting
- Token bucket algorithm smooths bursts better than fixed window

## Failure Scenarios

| Scenario                        | Handling                                          |
|---------------------------------|---------------------------------------------------|
| Duplicate payment request       | Redis idempotency key returns existing transaction|
| Merchant rate limit exceeded    | 429 Too Many Requests                             |
| payment-service down            | Circuit breaker opens at gateway, 503 returned    |
| fraud-service consumer crash    | Kafka retries, then DLQ after 3 attempts          |
| notification delivery fails     | @RetryableTopic — 3x with exponential backoff     |
| DB write fails mid-transaction  | Spring @Transactional rollback                    |
