# ADR-005: Confluent Cloud Over Local Kafka (Docker)

**Date:** 2024-01  
**Status:** Accepted  
**Deciders:** Data Engineering

---

## Context

SkyOps includes a streaming path that replays historical BTS Silver data chronologically through a Kafka topic (`flights.departures`) to demonstrate real-time operational monitoring. Spark Structured Streaming consumes this topic and writes 15-minute tumbling window aggregations to `skyops.gold.streaming_agg`.

Additionally, Avro serialisation with Schema Registry is used to demonstrate schema evolution safety — a common production pattern that distinguishes a senior-level portfolio project.

A Kafka broker, Schema Registry, and compatible consumer are required. Two approaches were evaluated.

---

## Options Considered

### Option A — Local Kafka in Docker (docker-compose)

**Pros:**
- Zero cost
- Full control over broker configuration
- Commonly seen in tutorials — familiar setup

**Cons:**
- Requires Docker on the development machine — violates the no-Docker design principle (ADR-001)
- Databricks Structured Streaming cannot connect to `localhost:9092` — requires a publicly accessible broker or Kafka Connect bridge
- Schema Registry running in Docker is on a separate network from Databricks — complex firewall/tunnel configuration required
- Docker `kafka + zookeeper + schema-registry` stack consumes ~2GB RAM on local machine
- Producer (replaying BTS data) must be run locally — not deployable as a Databricks job
- If local machine is off, streaming demo is unavailable
- ZooKeeper dependency (in older Kafka versions) adds operational complexity

### Option B — Confluent Cloud (Managed Kafka)

**Pros:**
- $400 free trial credit — covers all streaming development usage comfortably
- Publicly accessible broker endpoint — Databricks can connect directly over internet
- Schema Registry included (no separate setup)
- Basic cluster provides 6 partitions per topic — sufficient for the BTS replay volume
- Kafka producer can run as a Databricks Job — fully cloud-native, no local machine dependency
- Confluent Cloud UI shows topic activity, consumer group lag, schema versions — useful for portfolio screenshots
- After trial: Basic cluster at zero consumption = $0 (no messages = no charge)
- MSR (Multi-Schema Registry) and RBAC available if needed later
- Managed upgrades — no broker version maintenance

**Cons:**
- Confluent Cloud is proprietary — different from open-source Apache Kafka configuration
- $400 trial has 30-day expiry — must manage usage within trial period
- After trial expiry and credit exhaustion: Basic cluster costs ~$1.50/day if actively running
  - Mitigation: delete cluster when not developing; recreate in 2 minutes when resuming
- Slightly different client configuration (SASL/SSL required vs plain localhost:9092)

### Option C — AWS MSK (Managed Streaming for Kafka)

**Pros:**
- Native AWS integration (if Databricks is on AWS)
- VPC peering available for private connectivity

**Cons:**
- MSK minimum cost: ~$0.21/hour per broker × 3 brokers = ~$450/month — far exceeds budget
- Schema Registry not included — requires Confluent Schema Registry on EC2 or Glue Schema Registry
- Much more complex setup than Confluent Cloud

---

## Decision

**Option B — Confluent Cloud (Basic cluster).**

The $400 trial credit covers all streaming development usage across the 10-week project with significant margin. After trial expiry, the cluster is paused when not in active use — no idle cost.

The critical enabler is that Confluent Cloud provides a publicly accessible broker endpoint that Databricks Structured Streaming can reach directly via SASL/SSL, without any network bridging or VPN configuration. This makes the Kafka producer deployable as a native Databricks Job — consistent with the fully cloud-native architecture.

Schema Registry included in the trial demonstrates Avro schema evolution — a senior-level topic in data engineering interviews.

---

## Configuration

```python
# Confluent Cloud Kafka client config (used in both producer and consumer)
# Credentials stored in Databricks Secret Scope — never hardcoded
kafka_config = {
    "kafka.bootstrap.servers": "<cluster-id>.confluent.cloud:9092",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f"username='{dbutils.secrets.get(\"skyops-secrets\", \"confluent_api_key\")}' "
        f"password='{dbutils.secrets.get(\"skyops-secrets\", \"confluent_api_secret\")}';"
    ),
}

# Schema Registry config
schema_registry_config = {
    "url": "https://psrc-<id>.confluent.cloud",
    "basic.auth.credentials.source": "USER_INFO",
    "basic.auth.user.info": (
        f"{dbutils.secrets.get('skyops-secrets', 'sr_api_key')}:"
        f"{dbutils.secrets.get('skyops-secrets', 'sr_api_secret')}"
    ),
}
```

**Topics:**

| Topic | Partitions | Retention | Purpose |
|-------|------------|-----------|---------|
| `flights.departures` | 6 | 7 days | BTS Silver data replay → Structured Streaming |
| `flights.departures.dlq` | 3 | 30 days | Dead-letter queue for malformed records |

**Partition key:** `AIRLINE_CODE` — ensures all flights for a given airline land in the same partition, preserving temporal ordering within airline.

---

## Consequences

**Positive:**
- No Docker — producer runs as Databricks Job, consumer runs as always-on Databricks streaming job
- Schema Registry enables Avro serialisation with schema evolution demonstration
- Confluent UI screenshots (topic throughput, consumer lag, schema versions) enrich portfolio documentation
- Zero ongoing cost during non-development periods (pause cluster)

**Negative:**
- Confluent Cloud trial expires 30 days after activation — must begin streaming work within that window
- After trial: must actively manage cluster lifecycle to avoid idle charges
- SASL/SSL configuration is more verbose than localhost Kafka — additional setup step for reviewers trying to reproduce

**Cost Management:**
- Pause Confluent cluster when not actively developing streaming components
- Recreate cluster (2 minutes) when resuming
- All credentials stored in Databricks Secret Scope — rotation is straightforward

---

## Interview Note

When asked about Kafka setup: explain the cloud-native rationale. Then describe the SASL/SSL authentication pattern — this demonstrates familiarity with production Kafka configuration beyond tutorial `localhost:9092` setups. Mentioning Schema Registry and Avro (vs plain JSON) shows awareness of schema governance at the transport layer.

---

## References

- [Confluent Cloud Basic cluster pricing](https://www.confluent.io/confluent-cloud/pricing/)
- [Databricks Structured Streaming with Kafka](https://docs.databricks.com/structured-streaming/kafka.html)
- [Confluent Schema Registry with Spark](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html)
- [Avro schema evolution rules](https://avro.apache.org/docs/current/spec.html#Schema+Resolution)
