# Architectural Decision Records (ADRs)

Design notes and trade-offs for key technical choices:

* **[ADR-0001: Apache Iceberg as Open Table Format](file:///Users/satyamsingh/Documents/customer-data-platform/docs/adr/0001-apache-iceberg-table-format.md)** - Selection of Iceberg over Delta Lake and Apache Hudi.
* **[ADR-0002: Apache Spark (Scala) as Unified Engine](file:///Users/satyamsingh/Documents/customer-data-platform/docs/adr/0002-spark-scala-unified-engine.md)** - Single engine for streaming micro-batches and batch backfills.
* **[ADR-0003: Deterministic Event Identity for Ingestion](file:///Users/satyamsingh/Documents/customer-data-platform/docs/adr/0003-deterministic-replay-safe-ingestion.md)** - Replay safety via SHA-256 and key-sorted canonical JSON.
* **[ADR-0004: Integer Paise for Financial Ledgers](file:///Users/satyamsingh/Documents/customer-data-platform/docs/adr/0004-integer-paise-currency-representation.md)** - Eliminating floating-point drift using 64-bit integer paise.
