# Customer Data Platform (CDP) - Lakehouse Architecture

## 1. Summary

The platform ingests transactional databases, event streams, partner/vendor files, and third-party APIs into an immutable S3 landing zone and an Apache Iceberg lakehouse. Spark is the main processing engine, Kafka/Kafka Connect handles streaming and CDC, and Airflow orchestrates batch workflows and dependencies.

The core design principle is: **raw data is never overwritten, every downstream write is replay-safe, and every curated table exposes freshness, quality, lineage, and reconciliation status.**

At the stated scale (10K events/sec peak, ~500M file rows/day, 50M customers and five years of history), the design separates ingestion from transformation and separates analytical serving from operational application reads. The platform is not designed around one universal "exactly once" mechanism. Instead, it makes each source's delivery guarantee explicit and achieves **effectively-once lakehouse state through deterministic identity + idempotent commits + source-position tracking**.

## 2. Assumptions

1. AWS is the cloud environment; S3 is the durable object store.
2. Kafka is available for event streams and database CDC.
3. Source databases expose CDC through transaction log/CDC tooling; where CDC is unavailable, incremental extraction uses a monotonically increasing key or updated_at plus overlap.
4. Partner files arrive in S3/object storage or an SFTP landing service.
5. APIs provide a cursor/token or a stable incremental timestamp. If neither exists, the source is treated as snapshot ingestion and reconciled by content hash.
6. Five years of history remains queryable in Iceberg, with older raw objects transitioned to cheaper storage where policy permits.
7. Money is stored strictly as BIGINT paise, never floating point.
8. PII is classified and access-controlled at table/column level. Raw data is restricted to ingestion/engineering roles.

## 3. Architecture

**Transactional DBs → CDC/Kafka Connect → Kafka → Bronze Iceberg**

CDC records contain source name, table, primary key, operation, source transaction/sequence, event timestamp, schema version and ingestion timestamp. Kafka partitions by business primary key to preserve per-key ordering.

**Event streams → Kafka → Bronze Iceberg**

Events are retained in Kafka long enough to replay ingestion. The Bronze table stores the original payload plus ingestion metadata.

> **Concrete Entity Example: Real-Time Payment Transaction Event:**
> ```
> payment_txn_id: "TXN_99482103"              (Business Primary Key)
> payer_handle:   "customer@vpa"               (Hashed / masked in Silver)
> payee_handle:   "merchant_grocery@vpa"       (Merchant entity)
> merchant_id:    "M_8830192"
> amount_paise:   145000L                      (₹1,450.00 represented as BIGINT)
> txn_status:     "SUCCESS"
> txn_timestamp:  2025-01-15T10:14:22Z
> source_id:      "PAYMENT_GATEWAY_NODE_4"
> ```
> This record arrives via Kafka (10K events/sec peak), is ingested append-only into Bronze with deterministic metadata, cleansed and deduplicated into Silver, and aggregated into merchant settlement ledgers in Gold.

**Files → S3 landing → manifest/checksum validation → Bronze Iceberg**

Files are first copied without transformation. A manifest records object URI, size, checksum, arrival time and source delivery ID. A file is processed only after validation and can be safely retried because the delivery ID/checksum is unique.

**APIs → Airflow/Spark extract → S3 landing → Bronze**

The extraction state stores the source cursor/watermark. The next request begins from the committed cursor, but an overlap window is used where the API's ordering guarantee is weak.

### Lakehouse layers

- **Bronze:** immutable source-shaped data + ingestion metadata. No business joins.
- **Silver:** canonical schemas, type normalization, deduplication, CDC application, data-quality rules, PII classification.
- **Gold:** business entities and aggregates for reporting, finance and analytics.
- **Serving:** Trino/SQL endpoint for interactive analytics; selected application-facing datasets are materialized to an operational store rather than making OLTP applications query the lake directly.

Apache Iceberg is used for all managed lakehouse tables. It provides atomic commits, snapshots/time travel, schema evolution and partition evolution. It also avoids tying table semantics to physical directory layouts.

Table metadata and commit coordination are managed through an **Iceberg REST Catalog** (such as Apache Polaris or AWS Glue Data Catalog). The REST Catalog acts as the single source of truth for atomic Compare-And-Swap (CAS) snapshot commits, provides multi-engine interoperability between Spark and Trino, and supports credential vending so compute engines receive short-lived, table-scoped S3 access tokens rather than broad static IAM credentials.

## 4. Architectural Trade-Offs

### 1. Table Format: Apache Iceberg vs Delta Lake vs Apache Hudi
* **Chosen:** Apache Iceberg.
* **Why:** Iceberg decouples table metadata from any single compute engine. It works natively across Spark, Trino, and Flink without requiring Databricks runtime dependencies. Its partition evolution allows changing partition strategies (e.g. from daily to hourly) over 5 years of history without expensive table rewrites.
* **Rejected:**
  * *Delta Lake:* Optimized for Databricks ecosystems, but historically coupled to Databricks runtime features. Multi-engine write concurrency across open-source engines is less seamless.
  * *Apache Hudi:* Hudi offers mature record-level upserts, but catalog management and partition evolution are more complex to operate across multi-engine readers (like Trino).
* **What it costs:** Iceberg's v2 Merge-On-Read (MoR) generates positional delete files for CDC upserts. This shifts write cost to read cost, requiring periodic background compaction (`rewrite_data_files`).

### 2. Processing Engine: Apache Spark (Scala) vs PySpark vs Apache Flink
* **Chosen:** Apache Spark (Scala) for both streaming micro-batches and large batch transformations.
* **Why:** Spark runs natively on the JVM. Scala gives compile-time type safety for schemas and direct Dataset execution without Python-to-JVM serialization overhead. A single Spark Scala codebase handles both 10-30s streaming micro-batches and daily ~500M row batch jobs, keeping operations unified.
* **Rejected:**
  * *PySpark:* Incurs serialization overhead for custom hashing and scalar UDFs, and lacks static type checking for financial contracts.
  * *Apache Flink:* Flink is ideal for sub-second, stateful per-event streaming (like real-time fraud scoring). However, writing ACID lakehouse tables at 10K events/sec only requires 10-30s micro-batches. Running both Flink (streaming) and Spark (batch/backfills) would double operational overhead.
* **What it costs:** Spark Structured Streaming has higher baseline latency (~2-10 seconds) than Flink. If business requirements ever demand sub-second event writes, the streaming ingestion layer would need Flink.

### 3. Pre-Processing: Kafka Connect with Single Message Transforms (SMTs) vs Dedicated Spark Pre-ETL
* **Chosen:** Kafka Connect with SMTs for lightweight filtering, routing, and PII envelope masking before S3 landing.
* **Why:** Eliminates the need for an intermediate Spark streaming job purely for projection, column pruning, and dead-letter topic routing. Offloading this stateless transform to the Kafka Connect cluster dramatically simplifies the topology and cuts cluster infrastructure costs.
* **Rejected:**
  * *Separate Spark Streaming Pre-Job:* Spawning a dedicated Spark job just to filter incoming topics before dumping to S3 wastes executor cores on stateless pass-through operations.
* **What it costs:** SMTs are strictly stateless, single-record transformations. Any logic requiring stateful deduplication across records, joins, or historical lookups must still be deferred to the Spark Silver layer.

### 4. Orchestration: Apache Airflow vs Dagster / Prefect
* **Chosen:** Apache Airflow.
* **Why:** Industry standard for enterprise batch scheduling with battle-tested SparkSubmit, EMR, and Glue operators. Largest talent pool and mature integrations with AWS governance tooling.
* **Rejected:**
  * *Dagster / Prefect:* Dagster offers superior asset-oriented lineage models, but Airflow's institutional operational maturity and existing enterprise deployment make it lower risk.
* **What it costs:** Airflow's scheduler can become a bottleneck under thousands of dynamic DAG runs, and its metadata database requires careful pruning.

### 5. Medallion Layering (Bronze/Silver/Gold) vs Flat 2-Layer Model
* **Chosen:** 3-layer Medallion Lakehouse.
* **Why:** Silver provides a crucial, immutable contract boundary where CDC reconciliation, schema evolution, deduplication, and quality gates execute. Gold can then focus purely on domain aggregation and financial reconciliation without worrying about corrupt source payloads.
* **What it costs:** Storing data across Bronze, Silver, and Gold multiplies S3 storage volume. We mitigate this by establishing aggressive cold-storage lifecycle policies for Bronze raw files after 90 days.

## 5. Trust model

A table is never considered trusted simply because a Spark job completed without crashing. Every curated table must satisfy four criteria before downstream promotion:

* **Correctness:** Enforced schema contracts, unique primary keys, positive currency amounts, and nullability checks.
* **Currentness:** Freshness SLAs verified against the timestamps inside records rather than wall-clock run time.
* **Meaning:** Explicit field definitions and standardized data units (e.g. money strictly stored as integer paise).
* **Lineage:** Every row retains source metadata, pipeline run ID, and code version for deterministic auditability.

## 6. Hard problem #1: replay-safe ingestion

The platform does not depend on a perfect exactly-once source. Every record gets a deterministic identity based on source + entity + source sequence/id (or a canonical payload hash when the source lacks a stable ID).

> **Concrete Invariant:**
> For a payment transaction event:
> `_event_id = SHA256("payment_service" | "payment" | payment_txn_id)`
> If `payment_txn_id` is missing or blank whitespace, fallback is computed from the full payload with columns ordered alphabetically to guarantee canonical key-value JSON serialization:
> `_event_id = SHA256("payment_service" | "payment" | SHA256(canonical_sorted_json(payload)))`

The Bronze write is append-oriented using the Spark 3.5 Iceberg V2 DataFrame API (`writeTo.append()`). Silver application is idempotent using MERGE/primary-key semantics. A retry therefore produces the same final state instead of duplicate business rows.

For streaming, the checkpoint stores the Kafka progress, but the sink remains idempotent because a checkpoint can be lost/replayed and a source can resend messages.

For files, the manifest/checksum is the idempotency boundary. A file with the same delivery ID and checksum is a no-op; a changed object under the same delivery ID is rejected with a `DeliveryConflictException`. In production, file receipts are tracked in an atomic Iceberg manifest table (`cdp_catalog.audit.file_manifests`).

## 7. Hard problem #2: CDC ordering, duplicates and deletes

CDC is harder than ordinary append ingestion because the same key can have INSERT, UPDATE and DELETE events, and retries can duplicate events.

Each CDC record carries a monotonically ordered source position (transaction LSN/SCN + sequence). Silver keeps the latest accepted position per business key. A record is applied only if its source position is newer than the stored position.

> **Concrete Failure & Protection Scenario:**
> A merchant's KYC status transitions:
> `INITIATED` (Txn 100, Seq 1) → `PENDING` (Txn 100, Seq 3) → `VERIFIED` (Txn 100, Seq 5).
> Due to a Kafka partition consumer rebalance or network retry, the event for `PENDING` (Seq 3) arrives *after* `VERIFIED` (Seq 5) has already been committed to Iceberg.
>
> In an un-guarded merge, state would silently regress back to `PENDING`.
> Our predicate guard in `CdcMerge`:
> `WHEN MATCHED AND (s.transaction_id > t.transaction_id OR (s.transaction_id = t.transaction_id AND s.sequence_id > t.sequence_id))`
> explicitly rejects the stale Seq 3 event. State remains `VERIFIED`.

Deletes are represented as tombstones in Silver state rather than physically disappearing from Bronze. Gold models can decide whether deleted records should be excluded or retained historically.

The engine supports single and composite business keys (e.g., `(tenant_id, payment_id)`), and strictly filters out malformed records with NULL primary keys before the merge to prevent partition poisoning.

### Concurrency & Simultaneous Writes
When multiple Spark jobs or streaming micro-batches write to the same Iceberg table simultaneously, Iceberg avoids locking the table. Instead, it allows both jobs to write their data files in parallel. The REST catalog arbitrates atomic commits via Compare-And-Swap (CAS). If both jobs attempt to commit at the exact same millisecond, the first commit succeeds, and the second job automatically retries its commit against the new snapshot rather than failing the pipeline.

## 8. Hard problem #3: knowing that a table is trustworthy

A green checkmark on a Spark or Airflow job only means the code executed without crashing. It does not mean the data inside is correct. The job could have happily processed zero records, recorded negative balances, or introduced rounding errors.

To guarantee data trustworthiness across 500M daily records, the platform enforces four core principles:

1. **Strict Integer Money (No Floating-Point Drift):**
   Computers store decimal fractions imperfectly in binary (`0.10 + 0.20 = 0.30000000000000004`). Over 500 million transactions, tiny rounding errors compound into large ledger discrepancies. We strictly store all currency as `BIGINT` integer paise (1 Rupee = 100 Paise). ₹14.50 is stored as `1450L`. Whole integer arithmetic never drifts.

2. **Single-Pass Inspection (Minimizing I/O & Shuffles):**
   Running multiple separate queries on 500M rows (one query to count rows, another to find duplicates, another to check negative amounts) forces Spark to scan the dataset and trigger distributed shuffles multiple times. Our quality gate consolidates these checks into a **single distributed aggregation pass**, reading the dataset from S3 once and significantly reducing network and disk I/O.

3. **Freshness Measured from the Data, Not the Clock:**
   If an upstream payment service stops emitting events, an hourly ingestion job will process zero records, write zero rows, and report "Success (Green)". To prevent silent pipeline starvation, we measure freshness against the **timestamp inside the records** (`isFresh`). If the newest record in the batch is older than our 15-minute SLA, an alert is raised immediately.

4. **Quarantine via Dead Letter Queue (DLQ):**
   Corrupted rows (e.g. negative amounts or duplicate IDs) are blocked from entering Gold reporting tables. Instead of deleting them (which violates financial audit rules), they are diverted to a quarantine Dead Letter Queue (DLQ) table for engineering inspection.

Checks are strictly classified:
* **Blocking Failures:** Corrupted/negative currency, duplicate primary keys, or missing customer IDs. These trigger a `QualityGateException` and immediately block promotion to Gold business tables.
* **Warning Checks:** Non-critical statistical drift (e.g. slight deviation in daily transaction volume), which alerts the team without halting healthy data pipelines.

## 9. Failure and recovery

Production pipelines fail in three main ways. The architecture handles each safely:

1. **Worker Crashes & Network Outages:**
   * **Kafka & Spark failures:** Spark streaming resumes automatically from saved checkpoints. Idempotent Iceberg merges ensure that replayed batches never create duplicate records.
   * **Partial batch writes:** Iceberg commits are all-or-nothing (atomic). Downstream readers only see fully committed snapshots, never incomplete half-written data.

2. **Corrupted Input & Schema Changes:**
   * **Tampered or corrupt files:** Files failing manifest or checksum validation are halted in quarantine before ingestion.
   * **Upstream schema drift:** Raw data is always safely preserved in append-only Bronze. If a new column breaks Silver schemas, promotion is paused until contracts are updated.

3. **Human Errors & Backfills:**
   * **Bugs in business logic:** Because Bronze data is permanently immutable, any corrupted Silver or Gold table can be accurately re-computed by replaying history through fixed code.
   * **Rollbacks:** Iceberg's snapshot history allows the platform to instantly time-travel or roll back to the last known good state if bad data was committed.
   * **Backfills:** Historical backfills use the exact same transformation code and quality gates, writing to isolated table partitions before final promotion.

## 10. Cost and scaling

The primary way to scale cost-effectively is **not** to pay for larger Spark clusters. It is to **minimize the amount of data scanned**:

1. **Smart Partitioning (Read Only What You Need):**
   * Partition tables by **ingestion date/time**, never by individual customer IDs (partitioning across 50M customer IDs creates millions of tiny, inefficient files).
   * Date-based partitioning allows queries to read only the exact days requested, skipping unneeded data automatically.

2. **Storage Optimization & Compaction:**
   * **Compact small files:** Real-time streaming creates many small files. Scheduled background maintenance merges these into optimal 128MB Parquet files (`rewrite_data_files`) to keep read queries fast.
   * **Cold storage tiering:** Move raw Bronze S3 files older than 90 days to cheaper storage tiers (S3 Glacier), slashing long-term storage costs while preserving full audit history.

3. **Incremental Compute & Auto-Scaling:**
   * Run incremental merges on newly arrived records rather than re-computing entire historical tables.
   * Auto-scale Spark compute up during peak batch processing windows and scale down during off-peak hours.

## 11. Serving consumers

To protect lakehouse performance, **customer-facing applications never query the S3 lake directly**. Data is served according to consumer needs:

* **Interactive Analytics & BI (Trino / SQL):** Business analysts and dashboards query clean Gold tables and conformed Silver data.
* **Low-Latency Operational Apps (Redis / Key-Value Stores):** Customer-facing APIs query high-speed materialized caches (<10ms reads) populated from Gold tables, shielding the lakehouse from millions of concurrent user requests.
* **Machine Learning & Data Science:** Feature pipelines read point-in-time consistent snapshots from Silver and Gold for model training.
* **Finance & Audit:** Auditors query immutable Iceberg snapshots and table commit histories for mathematically verified, reproducible evidence.

## 12. Test strategy

Testing focuses on distributed edge cases and data correctness rather than simple line coverage:

1. **Replay & Ingestion Tests:** Verifies that re-running identical batches produces the exact same event IDs, and that altering a file's content under an existing delivery ID is rejected with a conflict error.
2. **CDC Ordering & State Tests:** Verifies that delayed older events cannot overwrite newer state, and that delete tombstones prevent deleted users from being resurrected.
3. **Quality & Financial Tests:** Verifies that duplicate payment IDs, negative money, and missing customer IDs halt the pipeline and quarantine bad data.
4. **Freshness & Contract Tests:** Verifies that a healthy pipeline reading stalled source data is flagged as stale, and that table names are sanitized against SQL injection.

## 13. Deliberate omissions & Scope boundaries

To keep the focus squarely on lakehouse correctness and invariants, secondary components are bounded:

* **Infrastructure Scaffolding:** Full Terraform configurations and fine-grained cloud IAM policies are omitted to focus on lakehouse data invariants.
* **Data Privacy (DPDP Act):** Personal data (phone numbers, IDs) is tokenized at the Silver boundary. When a user requests data erasure, the encryption key is destroyed, instantly rendering past raw logs unreadable while preserving historical financial ledger integrity.

## 14. Reference Implementation Architecture

The production reference implementation is written in **Apache Spark (Scala)** under `code/`:

* **[`Ingestion.scala`](file:///Users/satyamsingh/Documents/customer-data-platform/code/src/main/scala/com/cdp/lakehouse/Ingestion.scala):** Replay-safe Bronze ingestion, canonical sorted JSON hashing, and file delivery manifests.
* **[`CdcMerge.scala`](file:///Users/satyamsingh/Documents/customer-data-platform/code/src/main/scala/com/cdp/lakehouse/CdcMerge.scala):** Monotonic CDC state reconstruction, in-batch window deduplication, and soft-delete tombstones.
* **[`QualityGate.scala`](file:///Users/satyamsingh/Documents/customer-data-platform/code/src/main/scala/com/cdp/lakehouse/QualityGate.scala):** Single-pass quality verification, strict integer paise validation, and freshness SLAs.
* **[`LakehouseHardPartsSpec.scala`](file:///Users/satyamsingh/Documents/customer-data-platform/code/src/test/scala/com/cdp/lakehouse/LakehouseHardPartsSpec.scala):** Full ScalaTest test suite verifying all invariants hermetically.


