# PayStream — System Design Interview Q&A

## Scalability

**Q: How would you scale this to 10x traffic (10M transactions/day)?**

A:
1. **payment-service**: Horizontal scale to 10 pods behind load balancer. Kafka partitioning by merchantId ensures ordered processing per merchant.
2. **fraud-detection-service**: Scale consumers independently — increase partition count on `payment.initiated` and add consumer instances.
3. **MySQL**: Read replicas for `getTransaction` queries. Shard by merchantId for write scaling.
4. **Redis**: Redis Cluster for distributed idempotency and rate limiting.
5. **Kafka**: Increase partition count. Use separate consumer groups per fraud rule type for parallel processing.

---

**Q: How do you handle exactly-once delivery in Kafka?**

A:
- Producer: `enable.idempotence=true` + `acks=all` prevents duplicate publishes
- Consumer: Idempotency at application layer (Redis dedup key) handles duplicate deliveries
- For critical state mutations, transactional outbox pattern can be added

---

**Q: What if the fraud detection service is slow and creates lag?**

A:
- Monitor consumer group lag with Prometheus/Grafana
- Scale fraud-detection consumers horizontally (more pods = more Kafka partition consumers)
- Add timeout to fraud check — if no result in N seconds, default to `APPROVED` with low-confidence flag
- Future: async fraud scoring with ML model via separate scoring-service

---

**Q: How do you prevent the same transaction from being processed twice?**

A:
- Client sends `X-Transaction-Id` header (UUID)
- payment-service does `SET NX EX 86400` on Redis key `idempotency:{transactionId}`
- If key already exists → return existing transaction from MySQL (no re-processing)
- Kafka producer idempotence prevents duplicate events

---

**Q: How would you add a settlement service?**

A:
- settlement-service subscribes to `payment.approved` topic
- Batches approved transactions every N minutes
- Sends batch payout to bank via external API
- Publishes `payment.settled` event
- payment-service consumes `payment.settled` and updates final status

---

**Q: How do you handle schema evolution in Kafka events?**

A:
- Use Confluent Schema Registry with Avro (upgrade path from current JSON)
- Schema versioning with backward/forward compatibility rules
- Never remove fields — mark deprecated and add new fields

---

**Q: What's your observability strategy?**

A:
- **Metrics**: Micrometer → Prometheus → Grafana (request rate, error rate, latency p99, Kafka lag)
- **Logs**: Structured JSON logs → Kibana (transaction lifecycle tracing)
- **Traces**: Spring Cloud Sleuth + Zipkin (distributed trace across all 4 services)
- **Alerts**: PagerDuty on circuit breaker open, DLQ depth > threshold, consumer lag > 10K
