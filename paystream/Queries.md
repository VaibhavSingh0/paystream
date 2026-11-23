# PayStream — Complete Technical Reference & Interview Guide

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Overview](#2-architecture-overview)
3. [Every Microservice — Class by Class](#3-every-microservice--class-by-class)
4. [Every Technology — Why This, Not That](#4-every-technology--why-this-not-that)
5. [Every Design Pattern — Explained with Code](#5-every-design-pattern--explained-with-code)
6. [Kafka Deep Dive](#6-kafka-deep-dive)
7. [Failure Scenarios](#7-failure-scenarios)
8. [Scalability](#8-scalability)
9. [Security](#9-security)
10. [Interview Talking Points — Top 10 Q&A](#10-interview-talking-points--top-10-qa)

---

## 1. Project Overview

### What is PayStream?

PayStream is a **distributed, event-driven payment processing platform** that simulates what companies like Razorpay, PhonePe, Paytm, and Flipkart actually build internally. It accepts payment requests, processes them asynchronously, runs real-time fraud detection, and delivers notifications — all while being resilient to failures at every layer.

### What problem does it solve?

In a naive payment system, everything happens synchronously in one giant service:

```
Client → MonolithicApp → DB → FraudCheck → SendEmail → Return Response
```

Problems with this:
- If email sending is slow, the entire payment is slow
- If fraud check crashes, payments stop
- You can't scale fraud detection independently from payment acceptance
- No audit trail, no retry on failure, no replay

PayStream solves this by **separating concerns into independent services connected by events (Kafka)**, so each service can fail, scale, and be deployed independently.

### Why does this impress product companies?

Product companies (not services companies) build systems at scale where:
- Millions of transactions flow per day
- A single bug can cause financial loss
- Services must be independently deployable
- Failure in one part must not cascade to others

PayStream demonstrates you understand all of this — it's not a CRUD app, it's a system that reflects real fintech architecture.

---

## 2. Architecture Overview

### Full Request Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  CLIENT (Mobile App / Web / Merchant Dashboard)                  │
│  Sends: POST /api/v1/gateway/payments                           │
│  With:  JWT token in Authorization header                       │
│         X-Transaction-Id header (UUID for idempotency)          │
└──────────────────────────┬──────────────────────────────────────┘
                           │ HTTPS
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  payment-gateway-service  :8080                                  │
│  - Validates JWT token                                          │
│  - Checks circuit breaker (is payment-service healthy?)         │
│  - Routes request to payment-service via REST                   │
│  - Returns 503 with friendly message if circuit is open         │
└──────────────────────────┬──────────────────────────────────────┘
                           │ REST HTTP
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  payment-service  :8081                                          │
│  - Checks Redis: has this X-Transaction-Id been seen before?    │
│    YES → return existing transaction (idempotency)              │
│    NO  → continue                                               │
│  - Checks rate limit: has this merchant exceeded 100 req/min?   │
│    YES → return 429 Too Many Requests                           │
│    NO  → continue                                               │
│  - Saves transaction to MySQL with status = INITIATED           │
│  - Publishes PaymentEvent to Kafka topic: payment.initiated     │
│  - Returns 202 Accepted immediately (async processing)          │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Kafka publish
                           ▼
                  [Topic: payment.initiated]
                           │ Kafka consume
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  fraud-detection-service  :8082                                  │
│  - Consumes PaymentEvent from payment.initiated                 │
│  - Runs VelocityRule: >5 txns/min from same IP? → fraud         │
│  - Runs AmountAnomalyRule: amount > 100,000? → fraud            │
│  - Calculates fraud score (0.0 to 1.0)                          │
│  - Publishes FraudResultEvent to:                               │
│    payment.approved   (if not fraud)                            │
│    payment.fraud-flagged (if fraud detected)                    │
└──────────────┬──────────────────────────┬───────────────────────┘
               │ payment.approved         │ payment.fraud-flagged
               ▼                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  notification-service  :8083                                      │
│  - Consumes from BOTH payment.approved AND payment.fraud-flagged │
│  - Sends mock SMS/Email notification                             │
│  - Logs to MongoDB                                               │
│  - If processing fails: retries 3x with exponential backoff      │
│  - After 3 failures: event goes to DLQ topic                     │
│  - DLQReprocessConsumer handles dead letters separately          │
└──────────────────────────────────────────────────────────────────┘

All 4 services expose /actuator/prometheus
       │
       ▼
Prometheus scrapes every 15s
       │
       ▼
Grafana displays dashboards (request rate, error rate, Kafka lag)

Zipkin collects distributed traces across all 4 services
```

### Why async (Kafka) instead of synchronous REST chain?

If we used REST calls:
```
gateway → payment-service → fraud-service → notification-service → response
```
Total latency = sum of all services. If fraud-service is slow (say doing ML), the client waits.
With Kafka: payment-service returns `202 Accepted` immediately after publishing to Kafka.
The client gets a fast response. Fraud check happens in the background. Notification happens after that.
**The user doesn't wait for fraud detection.** This is how real payment systems work.

---

## 3. Every Microservice — Class by Class

---

### 3.1 payment-gateway-service

**Purpose:** This is the single entry point for all external traffic. No client talks directly to payment-service or fraud-detection-service. The gateway handles authentication and resilience before passing the request downstream.

**Why a separate gateway service?**
- Centralizes authentication — you don't want JWT validation logic in every service
- Acts as a shield — if payment-service is down, the gateway handles it gracefully (circuit breaker)
- Future-proof — you can add API versioning, request transformation, logging here without touching business logic

**What breaks if removed?** Clients would need to call payment-service directly, bypassing JWT validation. Circuit breaker protection disappears. Any future cross-cutting concerns (throttling, A/B routing) would need to be duplicated in every service.

---

#### `PaymentGatewayApplication.java`
The entry point. `@SpringBootApplication` does three things:
- `@Configuration` — this class can define Spring beans
- `@EnableAutoConfiguration` — Spring Boot automatically configures what it detects on the classpath (security, web, actuator, etc.)
- `@ComponentScan` — scans the package and sub-packages for `@Component`, `@Service`, `@Controller`, etc.

Nothing special here — it just starts the Spring container.

---

#### `JwtAuthFilter.java`
**What it is:** A Servlet filter that runs before every HTTP request reaches any controller.

**What it does step by step:**
1. Reads the `Authorization` header from the request
2. Checks if it starts with `Bearer ` — if not, passes the request through (unauthenticated)
3. Extracts the token (everything after `Bearer `)
4. Parses and validates the token using the secret key
5. If valid: extracts the username (`subject` claim) and sets it in Spring's `SecurityContext`
6. If invalid: returns `401 Unauthorized` immediately

**Why `OncePerRequestFilter`?** This base class guarantees the filter runs exactly once per request, even in complex dispatcher scenarios (like forward/include in Servlet spec).

**Why is the secret key in `application.yml`?** For simplicity in this project. In production, it would come from AWS Secrets Manager or HashiCorp Vault — never hardcoded.

---

#### `SecurityConfig.java`
**What it is:** Configures Spring Security — which URLs need authentication, which are public.

Key decisions:
- `SessionCreationPolicy.STATELESS` — no HTTP session is created. Every request must carry a JWT. This is correct for microservices.
- `/actuator/**` and `/api/v1/auth/**` are public — health checks and login don't require a token
- Everything else requires authentication

**Why disable CSRF?** CSRF (Cross-Site Request Forgery) protection is for browser-based session auth. Since we use stateless JWT, there's no session to forge. CSRF is irrelevant here.

---

#### `ResilienceConfig.java`
Configures the circuit breaker registry with custom settings:
- `failureRateThreshold: 50` — if 50% of calls fail, open the circuit
- `waitDurationInOpenState: 30s` — wait 30 seconds before trying again
- `slidingWindowSize: 10` — evaluate the last 10 calls to decide health
- `permittedNumberOfCallsInHalfOpenState: 3` — in half-open state, allow 3 test calls

---

#### `AppConfig.java`
Defines a `RestTemplate` bean. This is the HTTP client used to call payment-service. Defined as a `@Bean` so Spring manages its lifecycle and it can be injected anywhere.

**Why not WebClient?** WebClient is reactive (non-blocking). RestTemplate is simpler for traditional synchronous Spring MVC. For this project, simplicity wins. In a reactive stack (Spring WebFlux), WebClient would be preferred.

---

#### `GatewayController.java`
The main controller. Two endpoints:

1. `POST /api/v1/gateway/payments` — forwards payment request to payment-service
2. `GET /api/v1/gateway/payments/{transactionId}/status` — checks transaction status

**`@CircuitBreaker(name = "payment-service", fallbackMethod = "...")`**
This annotation wraps the method with Resilience4j circuit breaker logic. If payment-service calls start failing, Resilience4j opens the circuit and directly calls `paymentServiceFallback()` instead — returning a friendly 503 message rather than making a call that will likely fail anyway.

**Fallback methods:** `paymentServiceFallback` and `getStatusFallback` — they return `503 Service Unavailable` with a human-readable message. Without these, the client would get an ugly exception stack trace.

---

### 3.2 payment-service

**Purpose:** The core transaction service. Accepts payment requests, enforces business rules (idempotency, rate limiting), persists the transaction, and kicks off the async processing pipeline by publishing to Kafka.

**Why a separate service?** This is the most critical service — it owns the transaction data and must be independently scalable. Under Black Friday load, you scale this horizontally without touching fraud detection.

**What breaks if removed?** Nothing gets paid. The entire pipeline starts here.

---

#### `PaymentServiceApplication.java`
Main class. `@EnableAsync` enables Spring's async task execution — allows methods annotated with `@Async` to run in separate threads. Used in conjunction with virtual threads in Java 21.

---

#### `Transaction.java` (Entity)
A JPA entity — maps directly to the `transactions` table in MySQL.

Fields:
- `transactionId` — the UUID we use for idempotency. Marked `@Column(unique = true)` so the DB enforces uniqueness too (defense in depth)
- `status` — `@Enumerated(EnumType.STRING)` stores the enum name as a string ("INITIATED") not a number. This is safer — if you add enum values, the DB column meaning doesn't silently shift
- `@PrePersist` / `@PreUpdate` — JPA lifecycle callbacks. Set timestamps automatically before insert/update. No need to set them manually in business logic.

---

#### `TransactionStatus.java` (Enum)
Five states: `INITIATED → FRAUD_CHECK_PENDING → APPROVED / FRAUD_FLAGGED / FAILED`

This is the state machine of a transaction. Each Kafka event moves the transaction to a new state. Having this as an enum (not a free-text string) means the compiler catches typos at build time.

---

#### `PaymentRequest.java`
A simple POJO (Plain Old Java Object) representing the incoming HTTP request body. No `@Entity` annotation — this is not persisted to DB. It's a Data Transfer Object (DTO) — just carries data from the HTTP layer to the service layer.

---

#### `PaymentEvent.java`
The Kafka message payload. Represents what gets published to `payment.initiated` topic.

**Why a separate class from `PaymentRequest`?** The event carries additional fields (`eventType`, `timestamp`) that aren't in the request. Also, decoupling the wire format (Kafka) from the API contract (HTTP) means you can evolve them independently.

---

#### `TransactionRepository.java`
A Spring Data JPA repository. By extending `JpaRepository<Transaction, Long>`, you get these methods for free (no SQL needed):
- `save()`, `findById()`, `findAll()`, `delete()`, etc.

Custom methods:
- `findByTransactionId(String)` — Spring Data generates the SQL `SELECT * FROM transactions WHERE transaction_id = ?` automatically from the method name
- `existsByTransactionId(String)` — generates `SELECT COUNT(*) > 0 ...` — efficient existence check

---

#### `IdempotencyService.java`
**What it does:** Before processing a payment, checks if we've seen this `transactionId` before.

```java
Boolean wasAbsent = redisTemplate.opsForValue().setIfAbsent(key, "processed", TTL);
```

`setIfAbsent` = Redis `SET NX EX` command. This is atomic:
- If the key does NOT exist: sets it and returns `true` (wasAbsent = true → not a duplicate)
- If the key DOES exist: does nothing and returns `false` (wasAbsent = false → duplicate!)

**Why Redis?** This check runs on every single payment request. Redis handles millions of operations per second with sub-millisecond latency. A DB query for this would be orders of magnitude slower.

**Why 24-hour TTL?** Payment dedup windows are typically 24 hours. After that, a retry with the same ID would be treated as a new payment. This prevents the Redis key from growing forever.

---

#### `RateLimitService.java`
**What it does:** Limits each merchant to 100 requests per minute using the token bucket algorithm.

```java
buckets.computeIfAbsent(merchantId, this::newBucket)
```

A `ConcurrentHashMap` stores one `Bucket` per merchant. `computeIfAbsent` is atomic — thread-safe even under concurrent requests.

Each bucket has capacity 100, refills 100 tokens every 1 minute. Each `tryConsume(1)` removes one token. If empty → returns false → 429.

**Why `ConcurrentHashMap`?** Multiple threads may serve requests for the same merchant simultaneously. `HashMap` is not thread-safe — concurrent reads/writes can cause data corruption. `ConcurrentHashMap` is thread-safe and high-performance.

---

#### `PaymentEventPublisher.java`
Publishes `PaymentEvent` to Kafka topic `payment.initiated`.

```java
kafkaTemplate.send(topic, key, event).whenComplete(...)
```

- First argument: topic name
- Second argument: partition key (transactionId) — Kafka routes messages with the same key to the same partition, ensuring ordering per transaction
- Third argument: the event object (serialized to JSON by `JsonSerializer`)

`.whenComplete()` — handles the async result. If publishing fails, we log the error. In production, this would trigger a retry or alert.

---

#### `PaymentService.java`
The core business logic. Orchestrates: idempotency check → rate limit check → DB save → Kafka publish.

`@Transactional` on `initiatePayment` — if the DB save succeeds but something else fails, the transaction rolls back. Kafka publish is outside the DB transaction intentionally — if Kafka publish fails after DB save, the transaction is committed (we don't want to lose the record). The retry/idempotency layer handles re-publishing.

`updateTransactionStatus` is called by other services (via REST or future event consumers) to update the transaction state as the saga progresses.

---

#### `PaymentController.java`
REST controller. Two endpoints:
- `POST /api/v1/payments` — calls `PaymentService.initiatePayment()`
- `GET /api/v1/payments/{transactionId}` — returns current transaction state

**`X-Transaction-Id` header:** If the client doesn't send one, we generate a UUID. This is the idempotency key. Clients that want dedup control should send their own UUID.

Returns `202 Accepted` (not `200 OK`) because the payment is not complete — it's been accepted for async processing. This is semantically correct HTTP.

---

#### `RateLimitExceededException.java`
A custom exception. By defining our own exception, the controller can catch it specifically and return `429 Too Many Requests` instead of a generic `500 Internal Server Error`.

---

### 3.3 fraud-detection-service

**Purpose:** Consumes payment events, runs fraud detection rules, and publishes the verdict. Completely independent from payment-service — it has no DB dependency on MySQL, no direct HTTP call. It knows nothing about REST APIs.

**Why a separate service?**
- Fraud detection logic can be complex and slow (ML models, external lookups). Keeping it separate means payment acceptance is never slowed by fraud check latency.
- Fraud rules change frequently. Deploying a new rule shouldn't require redeploying payment-service.
- Can be scaled independently during high-fraud periods.

**What breaks if removed?** Payments would be accepted but never approved or flagged — notification-service would receive nothing. The saga stalls after `INITIATED`.

---

#### `FraudDetectionServiceApplication.java`
Standard Spring Boot main class. No special annotations needed — Kafka consumer listeners auto-start.

---

#### `PaymentEvent.java`
A mirror of the event published by payment-service. This is intentional — fraud-detection-service has its own copy of the event class rather than sharing a library.

**Why not a shared library?** Sharing a library creates a compile-time dependency between services — defeating the purpose of microservices independence. If payment-service changes the shared library, fraud-detection-service must redeploy too. With separate copies, they can evolve independently (within backward-compatibility bounds).

---

#### `FraudResultEvent.java`
The event published after fraud check. Contains:
- `transactionId` — links back to the original transaction
- `fraudulent` — boolean verdict
- `reason` — human-readable explanation ("Velocity limit exceeded: 7 txns/min from IP 10.0.0.1")
- `fraudScore` — 0.0 to 1.0 confidence score (useful for downstream ML or manual review)

---

#### `FraudRule.java` (Interface)
```java
public interface FraudRule {
    RuleResult evaluate(PaymentEvent event);
}
```

This is the **Strategy Pattern**. Each fraud rule implements this interface. The `FraudCheckConsumer` holds a `List<FraudRule>` and calls `evaluate()` on each. Adding a new rule = creating a new class implementing `FraudRule`, annotating with `@Component`. Zero changes to existing code. This is the **Open/Closed Principle** — open for extension, closed for modification.

---

#### `RuleResult.java`
Immutable result object from a fraud rule. Uses static factory methods:
- `RuleResult.fraud(reason, score)` — creates a fraud result
- `RuleResult.pass()` — creates a pass result

Why immutable? Rule results are created and immediately consumed. No need for mutation. Immutability prevents bugs from accidental modification.

---

#### `VelocityRule.java`
**What it does:** Counts how many transactions have come from the same IP address in the last 60 seconds. If more than 5, it's suspicious.

```java
Long count = redisTemplate.opsForValue().increment(key);
if (count == 1) {
    redisTemplate.expire(key, Duration.ofMinutes(1));
}
```

`increment` is atomic in Redis — no race condition even under concurrent requests. The TTL is set only on first creation (count == 1) so the window slides correctly.

**Why Redis for velocity?** This counter needs to be shared across all instances of fraud-detection-service. An in-memory counter would be per-instance — if you have 3 pods, each counts only 1/3 of the actual traffic. Redis is a shared, centralized counter.

---

#### `AmountAnomalyRule.java`
Simple threshold rule — transactions over ₹1,00,000 get a fraud score of 0.6. In production this would be dynamic, loaded from MongoDB (configurable per merchant). For now it demonstrates the rule engine pattern.

---

#### `FraudResultPublisher.java`
Publishes the fraud verdict to one of two topics based on `isFraudulent()`:
- `payment.approved` — clean transactions
- `payment.fraud-flagged` — suspicious transactions

The topic name is dynamic, determined at runtime. This is the routing logic of the choreography saga.

---

#### `FraudCheckConsumer.java`
The Kafka consumer. The `@KafkaListener` annotation registers this method to consume from `payment.initiated`.

**`@RetryableTopic`:** If `consume()` throws an exception (e.g., Redis is temporarily down), Kafka does NOT send the event to DLQ immediately. It retries:
- Attempt 1: immediate
- Attempt 2: after 1 second
- Attempt 3: after 2 seconds (multiplier = 2)
  After 3 failures: event goes to `payment.initiated.dlq`

This is automatic — no manual retry logic needed. Spring Kafka handles it.

**Rule evaluation loop:**
```java
for (FraudRule rule : fraudRules) {
    RuleResult result = rule.evaluate(event);
    if (result.isFraudulent() && result.getScore() > maxScore) {
        maxScore = result.getScore();
        fraudReason = result.getReason();
        isFraud = true;
    }
}
```
Runs all rules, picks the highest-confidence fraud signal. This means multiple rules can fire, but the most severe one wins.

---

### 3.4 notification-service

**Purpose:** Final step in the saga. Sends notifications (email/SMS) to customers and merchants when a payment is approved or flagged. Provides a full audit trail in MongoDB.

**Why a separate service?** Notification delivery is unreliable (email providers go down, SMS gateways throttle). Isolating it means failures here never affect payment processing. Also, notification logic is completely different from payment logic — different team could own it.

**What breaks if removed?** Customers don't get notified. Payments still process correctly, but there's no user-facing feedback and no audit log.

---

#### `PaymentResultEvent.java`
The event consumed by notification-service from both Kafka topics. Same structure as `FraudResultEvent` — a local copy for the same reasons explained in fraud-detection-service.

---

#### `NotificationLog.java`
MongoDB document class. `@Document(collection = "notification_logs")` maps to a MongoDB collection.

**Why MongoDB for logs?**
- Notification logs are unstructured — different notification types have different fields
- Write-heavy, rarely queried by complex joins
- Schema-free: easy to add new fields without migrations
- MongoDB's document model is perfect for audit logs

---

#### `NotificationService.java`
Builds the notification message and saves to MongoDB. In production, this would call an email provider (SendGrid, SES) or SMS gateway (Twilio). Here it uses `log.info()` to simulate — the infrastructure wiring is real, only the external API call is mocked.

---

#### `NotificationConsumer.java`
Listens to BOTH `payment.approved` AND `payment.fraud-flagged` in one `@KafkaListener`. This is intentional — notification behavior differs based on `event.isFraudulent()`, but the consumer is the same.

Has the same `@RetryableTopic` configuration — 3 retries with exponential backoff before DLQ.

---

#### `DLQReprocessConsumer.java`
Listens to the DLQ topics: `payment.approved.dlq` and `payment.fraud-flagged.dlq`. Events land here after all retries are exhausted.

In production, this would:
1. Save to a "failed notifications" table for manual review
2. Trigger a PagerDuty alert
3. Provide a UI for ops team to manually reprocess

For now it logs the failure — the infrastructure is real, the downstream action is a placeholder.

---

## 4. Every Technology — Why This, Not That

---

### Java 21

**Why 21, not 17?**
Java 21 is an LTS (Long-Term Support) release and introduced **Virtual Threads** (Project Loom) as a stable feature. Virtual threads are lightweight threads managed by the JVM, not the OS. Traditional OS threads consume ~1MB of stack memory each. Virtual threads consume ~few KB.

**What this means for PayStream:**
In `payment-service`, handling 1000 concurrent requests traditionally needs 1000 OS threads = ~1GB RAM just for thread stacks. With virtual threads, 1000 concurrent requests use 1000 virtual threads mapped to a handful of OS threads = much lower memory footprint.

```java
// application.yml or programmatic config
spring.threads.virtual.enabled=true  # enables virtual threads for Tomcat
```

**Trade-off:** Virtual threads work best for I/O-bound workloads (waiting on DB, Redis, Kafka). For CPU-bound work, they don't help — OS threads are still used under the hood.

---

### Spring Boot 3

Spring Boot is a convention-over-configuration framework. It auto-configures everything based on what's on the classpath:
- See `spring-kafka` jar? Auto-configure KafkaTemplate and KafkaListenerContainerFactory
- See `spring-data-jpa`? Auto-configure DataSource, EntityManagerFactory, TransactionManager
- See `micrometer-registry-prometheus`? Auto-expose `/actuator/prometheus` endpoint

**Why Spring Boot 3 over 2?**
- Requires Java 17+ (aligns with Java 21 usage)
- Native compilation support (GraalVM)
- Jakarta EE 9+ (package names changed from `javax.*` to `jakarta.*`)
- Better Micrometer integration out of the box

**Trade-off:** Spring Boot is "magic" — it does a lot implicitly. For a team new to Spring, debugging auto-configuration issues can be tricky. The benefit is massive productivity.

---

### Apache Kafka

**What it is:** A distributed event streaming platform. Think of it as a persistent, high-throughput message log that multiple services can publish to and consume from.

**Why Kafka over RabbitMQ?**

| Concern | Kafka | RabbitMQ |
|---|---|---|
| Message retention | Keeps messages for configurable duration (days/weeks) — replayable | Deletes after consumption — not replayable |
| Throughput | Millions of messages/sec | Hundreds of thousands/sec |
| Consumer model | Pull-based, consumers control pace | Push-based, broker pushes to consumers |
| Ordering | Guaranteed within a partition | Only with single-consumer queues |
| Use case | Event streaming, audit logs, high throughput | Task queues, RPC, low-latency routing |

**Why not REST between services?**

If fraud-service was down and payment-service called it via REST:
- Payment-service gets an error
- Must implement retry logic itself
- Must handle timeouts
- Must handle the case where fraud-service partially processed the request

With Kafka:
- Payment-service publishes and forgets
- Fraud-service consumes when it's ready (even if it was down for 10 minutes)
- No tight coupling, no retry logic in the publisher

**Trade-off of Kafka:** Operational complexity. Kafka needs Zookeeper (or KRaft in newer versions). It's another system to monitor, tune, and operate. For simple systems, RabbitMQ is easier to run.

---

### Redis

**What it is:** An in-memory key-value store with optional persistence.

**Used for two things in PayStream:**
1. **Idempotency keys** — has this transactionId been processed?
2. **Velocity counters** — how many transactions from this IP in the last minute?

**Why not in-memory HashMap?**

```java
// WRONG for distributed systems:
private Map<String, Boolean> seen = new HashMap<>();
```

If you run 3 instances of payment-service (for scaling), each has its own HashMap. A duplicate request could land on a different instance and pass the check. Redis is a shared external store — all instances check the same data.

**Why not store idempotency keys in MySQL?**
MySQL queries involve:
1. Network call to DB
2. SQL parsing
3. Disk I/O (even with indexes)
4. Lock management

Redis is in-memory, single-operation, sub-millisecond. For something that runs on every payment request, speed matters.

**Trade-off:** Redis is an additional infrastructure component. If Redis goes down, idempotency and rate limiting break. Mitigated by Redis Sentinel/Cluster for high availability.

---

### MySQL

**What it is:** A relational database. Used in payment-service for storing transactions.

**Why MySQL over PostgreSQL?**
Both are excellent. MySQL was chosen because:
- More common in fintech for transactional workloads in India
- Slightly simpler operational story (wider DBA familiarity)
- Spring Boot/JPA works identically with both

**Why relational here (not NoSQL)?**
Transactions are **structured** data with strict schema requirements:
- Every transaction has the same fields
- Amount cannot be null
- transactionId must be unique
- We want ACID guarantees — if the payment is saved, it's really saved

NoSQL (MongoDB) sacrifices some ACID properties for flexibility. For financial transactions, schema enforcement and ACID are non-negotiable.

**Why not NoSQL like Cassandra for payment-service?**
Cassandra is eventually consistent — you might read a stale value. For payments, we need to know definitively whether a transaction exists before allowing a duplicate. Eventually consistent reads are dangerous here.

---

### MongoDB

**Where it's used:** fraud-detection-service (fraud rule configs) and notification-service (notification logs).

**Why MongoDB here and not MySQL?**

Notification logs are a good fit for MongoDB because:
- Schema varies by notification type (fraud alert has different fields than approval)
- Append-only workload — logs are written, rarely updated
- No complex joins needed — each log is self-contained
- Easy to add new fields as notification types grow

**Why not Elasticsearch for logs?** Elasticsearch is better for search and analytics. MongoDB is simpler to operate and sufficient when you just need to store and retrieve logs by transactionId.

**Trade-off:** MongoDB offers weaker ACID guarantees than MySQL (though it's improved significantly). Don't use it for financial ledger data. Use it for flexible, document-shaped data like logs and config.

---

### Resilience4j

**What it is:** A fault-tolerance library for Java. Implements circuit breaker, retry, rate limiter, bulkhead patterns.

**What is a circuit breaker?**

Named after electrical circuit breakers that trip to prevent damage. Three states:

```
CLOSED (normal)
│  All calls pass through
│  Track failure rate over sliding window
│  If failure rate > threshold:
▼
OPEN (tripped)
│  All calls fail immediately (no actual call made)
│  Return fallback response
│  Wait for waitDurationInOpenState (30s)
▼
HALF-OPEN (testing)
   Allow N test calls through
   If they succeed: → CLOSED
   If they fail: → OPEN again
```

**Why needed?** Without a circuit breaker, if payment-service is down, every request to the gateway waits for the full timeout (say, 10 seconds) before failing. 100 concurrent users × 10 seconds = gateway itself becomes slow and runs out of threads. Circuit breaker "fails fast" — returns immediately without making a call that will likely fail.

**Why Resilience4j over Hystrix?**
Hystrix (Netflix) is in maintenance mode — no new features. Resilience4j is the modern replacement, designed for Java 8+, functional APIs, and no external dependencies.

---

### Bucket4j

**What it is:** A Java rate-limiting library implementing the token bucket algorithm.

**Token Bucket Algorithm:**
Imagine a bucket that holds 100 tokens. Every minute, 100 new tokens are added (refill). Each request consumes 1 token. When the bucket is empty, requests are rejected.

```
Bucket capacity: 100
Refill: 100 tokens per minute

Minute 1: 50 requests → bucket has 50 tokens left → all pass
Minute 1: 60 more requests → bucket has 50 - 60 = -10 → 10 rejected (429)
Minute 2 starts: bucket refills to 100 → all pass again
```

**Why token bucket over fixed window?**

Fixed window: "allow 100 requests per minute" — counted from :00 to :59.
Problem: a client can send 100 requests at :59, then 100 more at :00 = 200 in 2 seconds.

Token bucket: smooths this — tokens accumulate at a constant rate. Burst is limited to bucket capacity.

**Why in-process (not Redis-backed)?**
Bucket4j can be Redis-backed for distributed rate limiting. For this project, in-process is used for simplicity. The trade-off: if you have 3 instances of payment-service, each has its own bucket — a merchant could get 300 requests/min. For true distributed rate limiting, use Bucket4j with Redis backend.

---

### Docker + Kubernetes

**Docker:** Packages each service as a container — a self-contained unit with the JRE, the JAR, and all dependencies baked in. Run anywhere: developer laptop, CI server, cloud. The `Dockerfile` describes how to build the image.

```dockerfile
FROM eclipse-temurin:21-jdk-alpine AS builder
# Multi-stage build: build stage uses full JDK
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
# Final image uses only JRE (smaller, more secure)
COPY --from=builder /app/target/*.jar app.jar
```

Multi-stage build means the final Docker image doesn't contain Maven, the full JDK, or source code — just the JRE and the JAR. Smaller image = faster pulls, smaller attack surface.

**Kubernetes:** Orchestrates containers across many machines. `docker-compose` runs containers on one machine. Kubernetes runs them across a cluster of machines and handles:
- Scheduling (which machine runs which container)
- Self-healing (restart crashed pods)
- Scaling (increase replicas when CPU is high)
- Service discovery (payment-gateway finds payment-service by DNS name, not IP)
- Rolling updates (deploy new version with zero downtime)

**Why both?** Docker = packaging standard. Kubernetes = deployment platform. They're complementary, not alternatives.

---

### Prometheus + Grafana

**Prometheus:** Scrapes metrics endpoints (`/actuator/prometheus`) from all services every 15 seconds. Stores time-series data: "at 14:32:15, payment-service handled 342 requests/sec with p99 latency of 45ms."

**Grafana:** Queries Prometheus and renders dashboards — graphs, gauges, alerts.

**Why not just logs?**
Logs tell you *what happened* ("ERROR: NullPointerException at PaymentService.java:42").
Metrics tell you *how the system is behaving* ("error rate jumped from 0.1% to 15% at 14:32").

You need both. Logs for diagnosis. Metrics for detection. Without metrics, you're flying blind — you don't know something's wrong until a user complains.

**Key metrics to watch in PayStream:**
- `http_server_requests_seconds` — request rate and latency per endpoint
- `kafka_consumer_lag` — how far behind is fraud-detection-service? High lag = processing bottleneck
- `resilience4j_circuitbreaker_state` — is the circuit open?
- `jvm_memory_used_bytes` — memory pressure per service

---

### Zipkin (Distributed Tracing)

**The problem it solves:** A request comes into the gateway, goes to payment-service, gets published to Kafka, consumed by fraud-service, then notification-service. If something is slow, which service is the culprit? Logs from 4 services don't tell you easily — each has its own log stream.

**What Zipkin does:** Assigns a `traceId` to every request at the gateway. This ID propagates through all HTTP calls and Kafka messages. Each service reports a "span" (a timed segment of work) to Zipkin. Zipkin stitches them together into a waterfall diagram:

```
[gateway: 2ms] → [payment-service: 45ms] → [fraud-detection: 180ms] → [notification: 12ms]
                                                      ↑
                                               This is your bottleneck
```

**Why Spring Cloud Sleuth?** Sleuth auto-instruments Spring Boot — it automatically propagates trace IDs through `RestTemplate`, Kafka messages, and log statements. Zero boilerplate.

---

## 5. Every Design Pattern — Explained with Code

---

### 5.1 Idempotency Pattern

**What is idempotency?** An operation is idempotent if doing it multiple times has the same effect as doing it once. `DELETE /payment/123` should return the same result whether called once or five times.

**Why critical for payments?** Networks are unreliable. A client sends a payment request. The server processes it and saves to DB, but the response is lost in transit. The client, seeing no response, retries. Without idempotency: the customer is charged twice.

**How PayStream implements it:**

```
Client sends: POST /payments with header X-Transaction-Id: uuid-abc-123

payment-service:
  key = "idempotency:uuid-abc-123"
  result = Redis SET key "processed" NX EX 86400
  
  if result == true:  // key was absent, first time we've seen this
    → process payment normally
    
  if result == false: // key already existed, duplicate request
    → fetch existing transaction from MySQL by transactionId
    → return it (same response as first time)
    → no charge, no Kafka event, nothing
```

The client gets the same response both times. The customer is charged once. The idempotency key expires after 24 hours — after that, the same transactionId would be treated as a new payment (acceptable for most payment windows).

---

### 5.2 Saga Pattern (Choreography)

**What is a Saga?** A distributed transaction spanning multiple services. In a monolith, you use a database transaction (`BEGIN` → do work → `COMMIT`). In microservices, you can't use a single DB transaction across services — each service has its own DB.

The Saga pattern is a sequence of local transactions. Each service does its work and publishes an event. The next service reacts to that event.

**Choreography vs Orchestration:**

**Orchestration:** A central "orchestrator" service tells each service what to do:
```
Orchestrator → "Hey fraud-service, check this payment"
Orchestrator → "Hey notification-service, send this email"
```
Pro: easy to see the flow in one place. Con: orchestrator is a single point of failure.

**Choreography (what PayStream uses):** Services react to events. No central controller:
```
payment-service publishes: payment.initiated
fraud-service sees it, does its work, publishes: payment.approved
notification-service sees it, does its work
```
Pro: no single point of failure, services are fully independent.
Con: the flow is implicit — you have to read all services to understand it.

**Why choreography here?** PayStream has only 4 services. The simplicity benefit of choreography outweighs the explicitness benefit of orchestration. For complex sagas with 10+ steps and compensating transactions, orchestration becomes clearer.

---

### 5.3 Dead Letter Queue (DLQ) Pattern

**What is a DLQ?** A special queue/topic where messages go when they can't be processed after all retries.

**Why needed?** If `notification-service` throws an exception while sending an email, we don't want to lose that message forever. But we also don't want to retry endlessly and block processing of new messages.

**PayStream's DLQ flow:**
```
1. notification-service receives payment.approved
2. External email API is down → exception thrown
3. Spring Kafka retries after 2 seconds (attempt 2)
4. Still failing → retries after 4 seconds (attempt 3, multiplier=2)
5. Still failing → event written to payment.approved.dlq
6. DLQReprocessConsumer consumes from .dlq
7. Logs the failure, persists for manual review / alerting
```

**What's in the DLQ:** The original message, unchanged. This means you can fix the bug, then replay the DLQ to process those messages.

**Without DLQ:** Failed messages would either be lost (bad) or block the consumer forever (also bad). DLQ is the correct middle ground.

---

### 5.4 Circuit Breaker Pattern

**States explained:**

**CLOSED (normal operation):**
Every call from gateway to payment-service goes through. Resilience4j tracks outcomes (success/failure) in a sliding window of 10 calls. If failure rate exceeds 50% → opens the circuit.

**OPEN (tripped):**
No calls go to payment-service. All requests immediately hit the fallback: return 503 "Service temporarily unavailable." Why? If payment-service is down, calling it wastes time and resources. Fail fast is better than slow failure. After 30 seconds → moves to half-open.

**HALF-OPEN (testing recovery):**
Allows 3 test calls through to payment-service. If they succeed → back to CLOSED. If they fail → back to OPEN for another 30 seconds.

**Why this matters:** Prevents "cascade failure." Without circuit breaker, gateway's thread pool fills up with threads waiting for the dead payment-service. Gateway becomes slow. Users see timeouts everywhere. With circuit breaker, gateway fails fast and stays healthy even when downstream is down.

---

### 5.5 Event-Driven Architecture

**Push vs Pull:**

Push (REST polling): "Hey fraud-service, check transaction 123... ok, now check 124... ok, now 125..."
Pull (Kafka): payment-service publishes to Kafka. fraud-service pulls messages when ready.

**Why pull (Kafka) is better for this case:**
- fraud-service controls its own consumption rate. If it's under load, it consumes slower — naturally backpressures
- payment-service doesn't need to know fraud-service's address
- If fraud-service restarts, it picks up where it left off (Kafka retains messages)

**Topic design decisions:**

`payment.initiated` — single topic for all initiated payments. fraud-service only needs one consumer.

`payment.approved` and `payment.fraud-flagged` — split into two topics instead of one `payment.result` topic. Why? Consumers can subscribe to only what they care about. A future "settlement-service" only needs `payment.approved`. It doesn't need to filter out fraud events.

---

### 5.6 Rate Limiting (Token Bucket)

**Visual explanation:**
```
Merchant bucket (capacity=100):
████████████████████████████████████████ (100 tokens at start of minute)

After 60 requests:
████████████████████████████████████████ (40 tokens left)

After 100 more requests:
 (0 tokens) → 60 requests rejected with 429

After 1 minute:
████████████████████████████████████████ (refilled to 100)
```

**Why per-merchant, not per-IP?** A legitimate merchant might have many customers on different IPs. Rate limiting per IP would block genuine customers. Rate limiting per merchantId limits misbehaving merchants without affecting others.

---

## 6. Kafka Deep Dive

### Topics in PayStream

| Topic | Partitions | Purpose |
|---|---|---|
| `payment.initiated` | auto-created | All new payments → fraud check |
| `payment.approved` | auto-created | Clean payments → notification |
| `payment.fraud-flagged` | auto-created | Suspicious payments → notification + alert |
| `payment.approved.dlq` | auto-created by Spring | Failed notification retries |
| `payment.fraud-flagged.dlq` | auto-created by Spring | Failed fraud-alert retries |

### Partitions and Ordering

A topic is split into partitions. Each partition is an ordered, immutable log. Messages with the same key always go to the same partition.

In PayStream:
```java
kafkaTemplate.send("payment.initiated", event.getTransactionId(), event);
//                  topic               key (partition routing)   value
```

Using `transactionId` as the key means all events for the same transaction go to the same partition → processed in order. This matters if a transaction has multiple events (initiated, then a status update).

### Consumer Groups

A consumer group is a set of consumer instances that together consume a topic. Each partition is consumed by exactly one instance in the group.

```
payment.initiated (3 partitions)
  Partition 0 → fraud-detection-service instance 1
  Partition 1 → fraud-detection-service instance 2
  Partition 2 → fraud-detection-service instance 3
```

If you scale fraud-detection-service to 3 pods, each pod gets 1 partition. 3x throughput. This is Kafka's horizontal scaling model.

### Consumer Offsets

Kafka tracks "how far has each consumer group read?" as an offset (message number). When fraud-detection-service restarts, it resumes from its last committed offset — no messages are lost or reprocessed (unless you reset the offset intentionally for replay).

`auto-offset-reset: earliest` in `application.yml` means: if this consumer group is new (no committed offset), start from the beginning of the topic. Used for new deployments.

### What happens if Kafka goes down?

- payment-service: `kafkaTemplate.send()` will fail. The exception is logged. The transaction is already in MySQL with status `INITIATED`. On restart, payment-service won't automatically retry publishing. In production, you'd use the **Transactional Outbox Pattern**: save the event to a DB table atomically with the transaction, then a background job reads and publishes from the DB.
- fraud-detection-service: stops consuming. Lag accumulates. When Kafka recovers, it picks up from last offset — processes everything it missed.
- notification-service: same as fraud-detection-service.

The fact that fraud-service and notification-service are catch-up consumers (they process from last offset) makes the system inherently resilient to Kafka downtime.

---

## 7. Failure Scenarios

### Scenario 1: Duplicate payment request

```
Client sends payment twice with same X-Transaction-Id: txn-abc
Request 1: Redis SET NX → success → process → return 202
Request 2: Redis SET NX → key exists → return existing transaction
Result: charged once, same response both times
```

### Scenario 2: Merchant rate limit exceeded

```
merchant-001 sends 101st request in one minute
RateLimitService.isAllowed() → bucket empty → return false
PaymentController → catch RateLimitExceededException → return 429
Result: merchant gets 429, retries after back-off
```

### Scenario 3: payment-service crashes mid-request

```
Payment saved to MySQL, Kafka publish in progress
Service crashes
On restart: transaction is in MySQL with status INITIATED
Client retries with same X-Transaction-Id
Redis key may or may not be set (race condition)
Result: idempotency key may prevent reprocessing OR
        new transaction created (acceptable — MySQL unique constraint catches true duplicates)
```

### Scenario 4: fraud-detection-service consumer throws exception

```
fraud-service consuming payment.initiated
Redis call for velocity check times out → exception
@RetryableTopic catches it
Retries after 1s, 2s
Still failing → event goes to payment.initiated.dlq
Transaction stays in INITIATED state indefinitely
In production: DLQ consumer would trigger alert, ops team investigates
```

### Scenario 5: payment-service is down (gateway circuit breaker)

```
Gateway calls payment-service → timeout/error
5 out of last 10 calls failed → circuit OPENS
Next 100 requests: all immediately return 503 (no actual call)
After 30s: circuit HALF-OPEN
3 test calls succeed → circuit CLOSES
Normal operation resumes
Result: gateway stays healthy, users get clear error message
```

### Scenario 6: MySQL is down

```
payment-service tries to save transaction
JPA throws DataAccessException
Transaction rolled back (nothing in DB)
Controller returns 500 (payment-service lets this bubble up)
Idempotency key was set in Redis but no DB record
Client retries with same transactionId
Redis key still set (24h TTL)
Idempotency check says "duplicate" → tries to find in MySQL → finds nothing
Throws IllegalStateException
Result: bug! In production, you'd handle this case — if idempotency key exists but no DB record, delete the Redis key and reprocess
```

---

## 8. Scalability

### payment-gateway-service

**Bottleneck:** CPU for JWT validation, network for downstream calls.
**Scale:** Horizontal — add more pods. Stateless (no session, no shared in-process state). Load balancer distributes requests.
**Kubernetes:** `kubectl scale deployment payment-gateway-service --replicas=5`

### payment-service

**Bottleneck:** MySQL write throughput (transactions table), Redis (idempotency + rate limit).
**Scale:**
- payment-service: horizontal (stateless, shared Redis + MySQL)
- MySQL: read replicas for `getTransaction` queries. Write scaling via sharding by merchantId.
- Redis: Redis Cluster for distributed rate limiting and idempotency at scale.
- Kafka producer: add more partitions to `payment.initiated` for higher publish throughput.

### fraud-detection-service

**Bottleneck:** Kafka consumer throughput (how fast can it process events?).
**Scale:** Add more partitions to `payment.initiated`. Scale fraud-detection pods — each pod gets 1+ partitions. With 10 partitions and 10 pods, 10x throughput.
**Gotcha:** Number of pods can't exceed number of partitions — extra pods sit idle.

### notification-service

**Bottleneck:** External notification API rate limits (email/SMS providers).
**Scale:** Horizontal for Kafka consumer throughput. For external API limits, implement a token bucket on outbound calls, or use a dedicated email queue.

### Database Scaling Strategy

| Service | Current | At 10x Scale |
|---|---|---|
| payment-service | Single MySQL | MySQL + read replicas + write sharding |
| fraud-detection-service | Redis (single) | Redis Cluster |
| notification-service | MongoDB (single) | MongoDB ReplicaSet → Sharded cluster |

---

## 9. Security

### JWT (JSON Web Token) Flow

**What is a JWT?**
A JWT is a self-contained token with three parts:
```
header.payload.signature
eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJtZXJjaGFudC0wMDEifQ.ABC123...
```

**Header:** `{"alg": "HS256", "typ": "JWT"}` — algorithm used to sign
**Payload:** `{"sub": "merchant-001", "iat": 1700000000, "exp": 1700086400}` — claims (who, when, expiry)
**Signature:** `HMACSHA256(base64(header) + "." + base64(payload), secret)` — proves the token wasn't tampered with

**How the gateway validates:**
```java
Jwts.parser()
    .verifyWith(Keys.hmacShaKeyFor(jwtSecret.getBytes()))
    .build()
    .parseSignedClaims(token);  // throws if invalid or expired
```
If the token was modified (e.g., someone changed `merchant-001` to `merchant-999`), the signature won't match — validation fails, `401 Unauthorized`.

**Why stateless (no session)?** Sessions store state on the server:
```
Session: { sessionId: "abc123", userId: "merchant-001" }
```
Every instance of payment-gateway-service would need access to the same session store (another Redis). With JWT, the token carries all needed information — any gateway instance can validate any token independently.

**The secret key:** Currently in `application.yml`. In production: **never** store secrets in code or config files. Use AWS Secrets Manager, HashiCorp Vault, or Kubernetes Secrets with encryption at rest.

**JWT weaknesses:**
- Tokens can't be invalidated before expiry (no "logout" without a token blacklist)
- If the secret key leaks, all tokens can be forged
- Mitigation: short expiry (15 min), refresh tokens, key rotation

---

## 10. Interview Talking Points — Top 10 Q&A

---

**Q1: Walk me through what happens when a payment is made in PayStream.**

**A:** The client sends a POST to the payment-gateway with a JWT token and an idempotency key (UUID) in the `X-Transaction-Id` header. The gateway validates the JWT and routes the request through a Resilience4j circuit breaker to payment-service. payment-service first checks Redis for the idempotency key — if it's a duplicate, it returns the existing transaction. If it's new, it checks the merchant's rate limit using Bucket4j token bucket. If that passes, it saves the transaction to MySQL with status INITIATED, then publishes a `PaymentEvent` to the `payment.initiated` Kafka topic and returns `202 Accepted`. Independently, fraud-detection-service has been listening to that topic. It runs velocity and amount anomaly rules, then publishes either `payment.approved` or `payment.fraud-flagged`. Notification-service consumes both topics and sends mock notifications, logging them to MongoDB. If notification fails, it retries 3 times with exponential backoff before the event goes to DLQ.

---

**Q2: How do you handle the case where a payment is submitted twice?**

**A:** We use an idempotency key — a UUID the client sends in the `X-Transaction-Id` header. When payment-service receives a request, it does a Redis `SET NX EX 86400` (set if not exists, with 24-hour TTL) on the key `idempotency:{transactionId}`. If Redis returns false (key already existed), we know it's a duplicate. We fetch and return the existing transaction from MySQL without reprocessing. The client gets the same response as the first call. This handles both malicious duplicates and innocent retries due to network issues.

---

**Q3: Why did you use Kafka instead of calling fraud-detection-service directly via REST?**

**A:** Three reasons. First, decoupling — payment-service doesn't need to know fraud-service's address or whether it's available. Second, resilience — if fraud-service is down for a few minutes, the events accumulate in Kafka and are processed when it recovers, with no data loss. With REST, payment-service would get errors and need its own retry logic. Third, scalability — Kafka partitioning lets us scale consumers independently. We can add fraud-detection pods to increase throughput without touching payment-service. The trade-off is operational complexity — Kafka is another system to run. For low-volume systems, REST would be simpler.

---

**Q4: What is a circuit breaker and why did you add one?**

**A:** A circuit breaker prevents cascade failures. Without it, if payment-service goes down, every request to the gateway waits for the full timeout before failing. With 100 concurrent requests and a 10-second timeout, the gateway's thread pool fills up — the gateway itself becomes unresponsive. With Resilience4j circuit breaker: after 50% of the last 10 calls fail, the circuit opens. All subsequent calls immediately return a fallback (503) without actually calling payment-service. After 30 seconds, 3 test calls go through. If they succeed, the circuit closes. This keeps the gateway healthy and gives payment-service time to recover.

---

**Q5: How would you scale this system to handle 10 million transactions per day?**

**A:** Starting with the bottleneck. 10M/day = ~116 TPS average, but peaks could be 10x that = ~1,200 TPS.

For payment-service: scale horizontally to 10 pods. All pods share MySQL and Redis. MySQL may need read replicas for status queries and write sharding by merchantId for inserts.

For fraud-detection-service: increase `payment.initiated` partitions to 20. Scale to 20 pods — each consumes one partition. Consumer lag tells us if we need more.

For Redis: move to Redis Cluster for distributed idempotency and rate limiting.

For MySQL: at this scale, consider a connection pool (HikariCP, already built into Spring Boot) tuning. If write throughput is the bottleneck, consider sharding or moving to CockroachDB.

For Kafka: Kafka itself handles millions of messages/sec on 3 brokers. Tune `batch.size` and `linger.ms` on producers for throughput.

---

**Q6: What is the Saga pattern and why did you use choreography over orchestration?**

**A:** A Saga is a way to implement distributed transactions without a 2-phase commit. Each service does its local transaction and publishes an event for the next service. If something fails midway, compensating transactions undo the work.

Choreography: services react to events. No central controller. That's what PayStream uses — payment-service publishes, fraud-service reacts, notification-service reacts. Each service is completely independent.

Orchestration: a central "workflow" service tells each service what to do. Easier to understand the full flow, but the orchestrator is a single point of failure.

I chose choreography because PayStream's flow is linear and simple (4 steps). The lack of a central controller simplifies deployment and failure handling. If the flow had complex branching, compensation transactions, or rollback logic, orchestration with a tool like Temporal would be better.

---

**Q7: How do you ensure exactly-once payment processing?**

**A:** Truly exactly-once is very hard in distributed systems. PayStream achieves effectively-once through two mechanisms. On the producer side: Kafka producer `enable.idempotence=true` with `acks=all` prevents duplicate Kafka messages from producer retries. On the consumer side: application-level idempotency in payment-service (Redis dedup key). Even if fraud-detection-service processes the same Kafka message twice (due to rebalancing), the state machine in the transaction table prevents double-processing — a transition from APPROVED to APPROVED is a no-op.

---

**Q8: What happens if Redis goes down?**

**A:** Two things are affected: idempotency checks and rate limiting.

For idempotency: `setIfAbsent` will throw an exception. PaymentService will catch this and... currently, it doesn't handle it gracefully. In production, the right behavior is to either reject the payment with 503 (safe, no duplicate) or fall back to a DB-based idempotency check (slower but safe).

For rate limiting: `bucket.tryConsume()` will throw. The rate limit should fail open (allow the request) rather than fail closed (block everything). We'd wrap the rate limit check in a try-catch and allow the request if Redis is unavailable.

For velocity rules in fraud-detection: Redis down means velocity checks can't run. The service should fail open (allow the transaction, flag for review) rather than fail closed (block all payments during a Redis outage).

---

**Q9: How do you monitor this system in production?**

**A:** Three pillars: metrics, logs, and traces.

Metrics via Prometheus + Grafana: I'd watch request rate and p99 latency per service, error rate, Kafka consumer lag (key indicator of fraud-detection bottleneck), circuit breaker state, JVM heap and GC pressure. Alerts when consumer lag exceeds 10,000 messages or circuit breaker opens.

Logs via structured JSON → ELK stack (Elasticsearch + Kibana). Every log line includes `transactionId` and `traceId` as structured fields — you can search "show me all logs for transactionId=abc-123" across all services in one Kibana query.

Traces via Zipkin: for every payment, the full distributed trace shows which service took how long. Essential for debugging latency issues that span multiple services.

---

**Q10: What would you improve if you spent another week on this?**

**A:** Four things:

1. **Transactional Outbox Pattern** for Kafka publishing: currently, if Kafka is down when payment-service tries to publish, the event is lost. The outbox pattern saves the event to a DB table atomically with the transaction, then a background job reliably publishes it. This gives at-least-once delivery guarantee.

2. **Schema Registry + Avro**: currently using JSON for Kafka messages. JSON is schema-less — a producer can change a field name and silently break consumers. Schema Registry (Confluent) with Avro enforces schema compatibility — backward/forward compatible changes only. Critical for a multi-team environment.

3. **Distributed rate limiting**: currently Bucket4j is in-process per payment-service instance. With 5 instances, a merchant gets 500 req/min instead of 100. Moving to Bucket4j's Redis backend would give true distributed rate limiting.

4. **Saga compensation**: if fraud-service crashes after consuming but before publishing the result, the transaction stays in INITIATED forever. A timeout-based cleanup job should detect stale INITIATED transactions and move them to FAILED after N minutes, triggering a compensating notification.

---

*This document was written to accompany the PayStream codebase as a complete reference for understanding every technical decision, pattern, and trade-off in the system. For architecture diagrams, see `docs/HLD.md`. For scaling Q&A, see `docs/SYSTEM_DESIGN_QA.md`.*
