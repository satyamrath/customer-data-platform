# ADR-0002: Use Apache Spark (Scala) for Streaming and Batch

## Context

The platform processes both real-time event streams (10K events/sec via Kafka) and daily batch files (~500M rows). We evaluated whether to use a single unified engine (Spark) or a split architecture (e.g. Flink for streaming + Spark for batch), as well as Scala vs PySpark.

## Decision

We chose **Apache Spark (Scala)** as the unified engine for both streaming micro-batches and daily batch processing.

### Rationale
* **Direct JVM Execution:** Spark's core is written in Scala. Scala provides direct JVM Dataset execution without the serialization penalty of passing data between Python workers and the JVM.
* **Compile-Time Type Safety:** Scala provides static type checks for complex financial records, catching contract errors at build time.
* **Unified Operations:** Using a single engine for 10-30s streaming micro-batches and daily backfill jobs keeps deployment runbooks and operations unified.

## Alternatives Considered

* **PySpark:** Added overhead for custom hashing and scalar UDFs, and weaker static type safety for financial records.
* **Apache Flink:** Industry standard for sub-second, stateful per-event streaming. However, writing lakehouse tables at 10K events/sec does not require sub-second commits (10-30s micro-batches are standard). Running both Flink and Spark would double infrastructure complexity.

## Trade-offs

Spark Structured Streaming has higher baseline latency (~2-10 seconds) compared to true event-driven streaming engines like Flink. For lakehouse storage ingestion, this trade-off is well worth the operational simplicity.

