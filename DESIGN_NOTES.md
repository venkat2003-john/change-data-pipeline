# Design Notes and Trade-offs

## Processing design

1. Read CSV columns as strings to avoid accidental type coercion.
2. Validate required schema and business fields; route invalid rows to `rejected_events.csv` with reason codes.
3. Reject duplicate event IDs in the current batch and IDs already present in the persisted event history.
4. Append new valid events to the prior accepted history and sort deterministically by UTC timestamp and event ID.
5. Replay history to derive current customer state. DELETE removes the key; INSERT/UPDATE upsert attributes.
6. Reconcile input/accepted/rejected counts, unique history IDs, and current state against a deterministic history replay.
7. Stage output files and replace each destination file only after all staged files are written; publish a reconciliation report and run report.

## Idempotency and recovery

The output `event_history.csv` acts as a simple checkpoint. Missing or unexpected input columns are treated as schema drift and fail the batch with a failed run report. Invalid existing history also fails before publishing data outputs. Re-running the same batch does not append event IDs already recorded. If a run must be retried, retain the original input file and rerun it against the same output directory. Invalid and duplicate rows remain visible in the reject output for that run. Preserve/copy the output directory before manual backfills.

## Trade-offs

- CSV keeps the challenge self-contained and easy to inspect, but it does not provide multi-file transactions or safe concurrent writes. Each staged file is replaced atomically, but a crash between file replacements can leave a mixed output version.
- Rebuilding current state from the full event history is straightforward and makes late-arriving older events deterministic, but runtime and memory grow with history size.
- Timestamp ordering is preferred over file arrival order; event ID is used only as a stable tie-breaker when timestamps match.
- Event IDs are treated as immutable global identifiers. Reuse with different payload is rejected as a replay rather than silently overwriting history.

## Production hardening not implemented

For production, replace CSV checkpointing with transactional storage (for example, a relational database or lakehouse table), enforce unique event IDs at the storage layer, atomically publish output versions, and add concurrent/distributed ingestion controls. The local pipeline persists a best-effort failed-run report, but does not implement a durable transaction log or automatic rollback after a machine crash.

## Reference-key scope

The challenge sample contains only customer events and no separate account, product, or parent table. Therefore there is no external foreign key to validate. `customer_id` is the entity key and is required/nonblank; if the model is extended to reference other entities, those keys should be checked against a reference dataset and rejected with a dedicated reason code.
