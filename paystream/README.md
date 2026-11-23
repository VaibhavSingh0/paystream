# PayStream — Real-Time Payment Processing & Fraud Detection Platform

A production-grade distributed payment processing system built with Java 21, Spring Boot 3, Kafka, Redis, and Kubernetes.
Designed to demonstrate senior-level distributed systems engineering: event-driven architecture, fault tolerance, observability, and scalability patterns.

---

## Architecture

```
Client
  │
  ▼
[payment-gateway-service :8080]   ← JWT Auth · Circuit Breaker (Resilience4j) · Request Routing
  │
  ▼
[payment-service :8081]           ← Idempotency (Redis) · Rate Limiting (Bucket4j) · MySQL
  │
  └──► Kafka: payment.initiated
              │
              ▼
[fraud-detection-service :8082]   ← Velocity Rule · Amount Anomaly · Rule Engine
  │
  ├──► Kafka: payment.approved
  └──► Kafka: payment.fraud-flagged
              │
              ▼
[notification-service :8083]      ← Async Notifications · DLQ · MongoDB Audit Log
```

**Infrastructure:** Kafka · Redis · MySQL · MongoDB · Prometheus · Grafana · Zipkin

---

## Key Engineering Features

| Feature | Implementation |
|---|---|
| **Idempotency** | Redis `SET NX EX 86400` on UUID transaction key — prevents duplicate processing across restarts |
| **Rate Limiting** | Bucket4j token bucket — 100 req/min per merchant, in-process with no external dependency |
| **Circuit Breaker** | Resilience4j on gateway → payment-service, opens at 50% failure rate, 30s open window |
| **Exactly-Once Kafka** | Producer `enable.idempotence=true` + `acks=all` |
| **DLQ + Retry** | `@RetryableTopic` with 3 attempts, exponential backoff (2s → 4s → 8s), dead-letter consumer |
| **Choreography Saga** | Stateless event-driven flow: initiated → approved/flagged → notified |
| **Observability** | Micrometer + Prometheus + Grafana dashboard, Zipkin distributed tracing |
| **Health Checks** | Spring Actuator `/actuator/health` on all services, used by K8s readiness/liveness probes |
| **Java 21** | Virtual threads for non-blocking I/O in payment-service |

---

## Project Structure

```
paystream/
├── docker-compose.yml                   # Full local stack: Kafka, Redis, MySQL, MongoDB, Prometheus, Grafana
├── payment-gateway-service/             # Entry point: JWT auth, circuit breaker, routing
├── payment-service/                     # Core: transactions, idempotency, rate limiting, Kafka publish
├── fraud-detection-service/             # Rule engine: velocity + amount anomaly, Kafka consumer/producer
├── notification-service/                # Async notifications, DLQ handling, MongoDB audit
├── k8s/                                 # Kubernetes Deployment + Service YAMLs
├── observability/                       # Prometheus scrape config + Grafana dashboard
└── docs/
    ├── HLD.md                           # Architecture, Kafka topics, design decisions
    └── SYSTEM_DESIGN_QA.md             # Interview Q&A: scaling, exactly-once, schema evolution
```

---

## Quick Start

**Prerequisites:** Docker + Docker Compose

```bash
# Clone
git clone https://github.com/vaibhavsingh8630/paystream.git
cd paystream

# Start everything
docker-compose up --build

# Services available at:
# payment-gateway:   http://localhost:8080
# payment-service:   http://localhost:8081
# fraud-detection:   http://localhost:8082
# notification:      http://localhost:8083
# Grafana:           http://localhost:3000  (admin/admin)
# Prometheus:        http://localhost:9090
# Zipkin:            http://localhost:9411
```

---

## API Usage

### Initiate a Payment
```bash
curl -X POST http://localhost:8080/api/v1/gateway/payments \
  -H "Authorization: Bearer <jwt-token>" \
  -H "Content-Type: application/json" \
  -H "X-Transaction-Id: txn-$(uuidgen)" \
  -d '{
    "merchantId": "merchant-001",
    "customerId": "customer-123",
    "amount": 5000.00,
    "currency": "INR",
    "sourceIp": "192.168.1.100"
  }'
```

### Check Transaction Status
```bash
curl http://localhost:8080/api/v1/gateway/payments/{transactionId}/status \
  -H "Authorization: Bearer <jwt-token>"
```

### Trigger Rate Limit (send 6+ requests in 1 minute from same merchant)
```bash
for i in {1..7}; do
  curl -X POST http://localhost:8081/api/v1/payments \
    -H "X-Transaction-Id: txn-$RANDOM" \
    -H "Content-Type: application/json" \
    -d '{"merchantId":"merchant-001","customerId":"c1","amount":100,"currency":"INR","sourceIp":"10.0.0.1"}'
done
# 6th and 7th requests return 429 Too Many Requests
```

### Trigger Fraud Detection (velocity: 6+ txns/min from same IP)
```bash
for i in {1..7}; do
  curl -X POST http://localhost:8081/api/v1/payments \
    -H "X-Transaction-Id: txn-$RANDOM" \
    -H "Content-Type: application/json" \
    -d '{"merchantId":"m-'$i'","customerId":"c1","amount":100,"currency":"INR","sourceIp":"10.0.0.1"}'
done
# Transactions 6+ flagged as fraud → notification-service logs FRAUD ALERT
```

---

## Event Flow

```
1. Client sends POST /api/v1/gateway/payments
2. Gateway authenticates JWT, checks circuit breaker, forwards to payment-service
3. payment-service: checks idempotency → rate limit → saves to MySQL → publishes payment.initiated
4. fraud-detection-service: consumes payment.initiated → runs velocity + amount rules → publishes result
5. notification-service: consumes payment.approved OR payment.fraud-flagged → logs to MongoDB
   └── If processing fails: retries 3x with backoff → DLQ consumer handles dead letters
```

---

## Kafka Topics

| Topic | Producer | Consumer |
|---|---|---|
| `payment.initiated` | payment-service | fraud-detection-service |
| `payment.approved` | fraud-detection-service | notification-service |
| `payment.fraud-flagged` | fraud-detection-service | notification-service |
| `payment.approved.dlq` | Kafka (auto) | DLQReprocessConsumer |
| `payment.fraud-flagged.dlq` | Kafka (auto) | DLQReprocessConsumer |

---

## Kubernetes Deployment

```bash
# Apply all manifests
kubectl apply -f k8s/

# Check pods
kubectl get pods

# Services scale independently
kubectl scale deployment fraud-detection-service --replicas=3
```

---

## Observability

- **Grafana** at `localhost:3000` — JVM metrics, Kafka consumer lag, request rates, error rates
- **Prometheus** at `localhost:9090` — raw metrics from all 4 services via `/actuator/prometheus`
- **Zipkin** at `localhost:9411` — distributed traces across the full payment flow

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 21 (Virtual Threads) |
| Framework | Spring Boot 3.2, Spring Kafka, Spring Security |
| Messaging | Apache Kafka |
| Cache / Idempotency | Redis |
| Databases | MySQL (transactions), MongoDB (fraud rules, notifications) |
| Resilience | Resilience4j (circuit breaker), Bucket4j (rate limiting) |
| Observability | Prometheus, Grafana, Zipkin, Spring Actuator |
| Container | Docker, Kubernetes |
| Build | Maven |

---

## Why This Architecture?

**Why Kafka over REST for inter-service calls?**
Decouples services — fraud detection can be down without blocking payment intake. Enables event replay and audit trails.

**Why choreography saga over orchestration?**
No single point of failure. Each service owns its own state machine. Scales independently.

**Why Redis for idempotency over DB?**
O(1) `SET NX` with TTL. Works across multiple instances of payment-service. 24-hour dedup window matches payment window.

See [docs/HLD.md](docs/HLD.md) for full design decisions and [docs/SYSTEM_DESIGN_QA.md](docs/SYSTEM_DESIGN_QA.md) for scaling discussion.

---

## Author

**Vaibhav Singh** — Senior Backend Engineer  
6+ years building distributed Java microservices at Deloitte, Cognizant, Wipro  
[LinkedIn](https://linkedin.com/in/vaibhavsingh8630) · [GitHub](https://github.com/vaibhavsingh8630)
