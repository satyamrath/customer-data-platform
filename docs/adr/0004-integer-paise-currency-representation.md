# ADR-0004: Use 64-bit Integer Paise for Financial Calculations

## Context

Financial ledgers require exact mathematical precision. Using standard IEEE-754 floating-point numbers (`FLOAT`, `DOUBLE`) in distributed analytical engines introduces binary floating-point representation drift (e.g. `0.10 + 0.20 = 0.30000000000000004`). Across 500 million daily records, compounding rounding discrepancies create ledger mismatches.

## Decision

We store all monetary values strictly as 64-bit integer paise (`BIGINT`):

* 1 Rupee = 100 Paise (e.g. ₹1,450.00 is stored as `145000L`).
* Floating-point numbers are prohibited in financial schemas.
* Quality gates reject negative amounts (`amount_paise < 0`) and nulls, isolating corrupt records to the Dead Letter Queue.

## Alternatives Considered

* **Double / Float:** Rejected due to inherent precision loss.
* **Decimal(18, 2):** While SQL `DECIMAL` supports fixed precision, integer arithmetic (`BIGINT`) is faster in Spark JVM vectorized execution and avoids locale-specific decimal parsing issues across raw text feeds.

## Trade-offs

BI dashboards and user-facing reports must divide by 100.0 when formatting currency strings for display.

