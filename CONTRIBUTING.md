# Contributing

Thanks for your interest in contributing to this project. Here are a few guidelines to keep things smooth:

## Branching & Workflow

We follow standard GitHub flow:
1. Fork or branch off `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Make your changes and write unit tests for any new logic.
3. Verify that all tests pass locally:
   ```bash
   cd code
   sbt test
   ```
4. Push your branch and open a Pull Request against `main`.

## Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/):
* `feat`: New feature or ingestion pipeline component
* `fix`: Bug fix or quality check patch
* `docs`: Documentation, design docs, or ADR updates
* `test`: Adding or updating test cases
* `refactor`: Code reorganization without behavioral change
* `chore`: Build or dependency updates

Example: `feat(ingestion): add key-sorted json hashing for bronze`

## Core Data Invariants

When writing or modifying pipeline code, make sure to respect these core lakehouse rules:
* **Deterministic Identity:** Raw records must get deterministic IDs so replaying a batch produces the same final state.
* **Monotonic CDC:** State merges must be ordered by source sequence/transaction ID; delayed events should never overwrite newer state.
* **Strict Integer Money:** Store currency strictly as integer paise (`BIGINT`), never floating-point.
* **Single-Pass Quality Gates:** Consolidate data validations into a single aggregation pass to avoid unnecessary scans of large datasets.
