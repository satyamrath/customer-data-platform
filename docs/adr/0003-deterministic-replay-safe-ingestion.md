# ADR-0003: Deterministic Event Identity for Replay-Safe Ingestion

## Context

In at-least-once distributed pipelines, task retries, spot instance preemptions, and backfills cause the same data to be re-read. If records are assigned random surrogate keys (`UUID.randomUUID()`), retries create duplicate rows.

Additionally, upstream services emitting JSON often send keys in non-deterministic orders (e.g. `{"user_id": "u1", "amount": 100}` vs `{"amount": 100, "user_id": "u1"}`). Hashing raw JSON strings directly produces different hashes for identical payloads, causing silent duplicate explosion.

## Decision

We compute deterministic SHA-256 event IDs using canonical key-sorted JSON:

1. If a business primary key (`source_id`) is available and non-empty:
   `_event_id = SHA256(source + "|" + entity + "|" + source_id)`
2. If `source_id` is missing:
   The payload columns are sorted alphabetically before serialization, producing a canonical JSON string that is hashed:
   `_event_id = SHA256(source + "|" + entity + "|" + SHA256(canonical_sorted_json))`

For batch files, a manifest registry tracks `(source, delivery_id, size, sha256)`. Retransmissions with matching hashes are treated as safe no-ops; altered files under the same delivery ID throw a `DeliveryConflictException`.

## Alternatives Considered

* **Random UUIDs:** Rejected because it makes re-runs non-idempotent.
* **Raw String Hashing:** Fragile to key-order permutation and formatting changes.

## Trade-offs

Sorting and hashing payload structs adds minimal CPU overhead during Bronze ingestion, which is offset by eliminating duplicate records and ensuring exact replay safety.

