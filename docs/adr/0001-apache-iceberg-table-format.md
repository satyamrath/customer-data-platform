# ADR-0001: Choose Apache Iceberg as Open Table Format

## Context

We need an open table format to support high-velocity streaming writes (10K events/sec) and daily batch ingestion (~500M rows) on top of AWS S3. The storage layer must provide ACID transactions, atomic commits, time travel, schema evolution, and partition evolution over 5 years of retention.

We evaluated Apache Iceberg, Delta Lake, and Apache Hudi.

## Decision

We chose **Apache Iceberg** as the standard table format across Bronze, Silver, and Gold.

### Rationale
* **Engine Agnostic:** Decouples table metadata from any specific vendor runtime. It works natively with Spark, Trino, and Flink.
* **Partition Evolution:** Allows updating partition layouts (e.g., from daily to hourly) without expensive historical data rewrites.
* **Hidden Partitioning:** Users write natural queries against timestamp columns without needing physical partition filters.
* **Optimistic Concurrency Control:** Supports concurrent writes between streaming micro-batches and scheduled batch backfills safely.

## Alternatives Considered

* **Delta Lake:** Tightly aligned with the Databricks ecosystem. Multi-engine write concurrency across open-source Spark and Trino is less straightforward.
* **Apache Hudi:** Good upsert support, but catalog integration and reader compatibility (especially Trino) have historically been more complex to operate.

## Trade-offs

Iceberg's v2 Merge-On-Read produces positional delete files during high-frequency CDC merges. To keep read latency low, regular compaction jobs (`rewrite_data_files`) must be scheduled in the background.

