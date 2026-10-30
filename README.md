# Customer Change Data Capture (CDC) Pipeline

## Overview

A local, deterministic Python/Pandas pipeline that consumes customer `INSERT`, `UPDATE`, and `DELETE` events from CSV, validates them, preserves append-only event history, and publishes the latest customer state.

## Requirements

- Python 3.10+
- pandas
- numpy (for the synthetic generator)
- pytest (for tests)

## Setup

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
python -m pip install -r requirements.txt
```

## Run the sample pipeline

From the project root:

```bash
python -m src.pipeline
```

Outputs are written to `outputs/`:

- `event_history.csv` — append-only accepted event history, sorted by event timestamp and event ID.
- `current_state.csv` — latest state after replaying accepted history in timestamp order.
- `rejected_events.csv` — invalid, duplicate, or previously processed event IDs, with a rejection reason.
- `run_report.json` — run ID, UTC start/end, duration, source file, input/accepted/rejected/output counts, and run status.
- `reconciliation.json` — checks that accepted history counts reconcile, event IDs are unique, and current state matches a replay of history.

To call the pipeline directly:

```python
from src.pipeline import process_events
report = process_events("data/sample_events.csv", "outputs", run_id="my-run-001")
```

## Generate reproducible test data

Generate 100,000 events (fixed default seed 42):

```bash
python -m src.generate_events --rows 100000 --seed 42 --output data/generated_events.csv
```

Then run it:

```python
from src.pipeline import process_events
process_events("data/generated_events.csv", "outputs/generated_run", run_id="generated-100k")
```

The generator accepts configurable row count, seed, and output path. The same row count and seed produce identical event data.

## Data contract and processing rules

Required CSV columns: `event_id`, `customer_id`, `event_type`, `event_timestamp`, `name`, `email`. All are read as strings; event timestamps are parsed as UTC. Event types are case-normalized and must be `INSERT`, `UPDATE`, or `DELETE`.

- `event_id` is the globally unique event key. A repeated ID within a batch or already present in the output history is rejected.
- `customer_id` identifies the entity whose state changes.
- Events are ordered by UTC timestamp, then `event_id` for deterministic tie-breaking.
- Older events cannot overwrite newer state. A later DELETE removes the customer; an older late-arriving INSERT does not restore it.
- INSERT and UPDATE both upsert the supplied customer attributes. DELETE removes the customer from current state but remains in event history.
- Invalid required values and malformed timestamps are written to the reject file with reason codes. Extra or missing columns are treated as schema drift: the batch fails clearly and a failed run report is written.
- Replaying the same event IDs against the same output directory does not append duplicates.

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for field definitions and assumptions and [DESIGN_NOTES.md](DESIGN_NOTES.md) for trade-offs and recovery behavior.

## Tests and performance

```bash
python -m pytest tests -v
```

The test suite covers inserts, updates, deletes, late/out-of-order events, duplicate IDs within and across batches, invalid fields/timestamps, missing and unexpected columns, empty input, reruns, run metrics, reconciliation, generator determinism, and a 100,000-row generator check. A 10,000-event pipeline performance test is included.

A local benchmark on the development runtime processed 100,000 generated events (seed 42) in approximately 3.74 seconds of pipeline time (4.50 seconds wall time), with about 193 MB peak process memory. Results vary by machine and should be treated as a reference, not a service-level guarantee.

## Limitations / production considerations

- CSV files are used as a lightweight local checkpoint. Output files are staged before replacement and each individual file replacement is atomic on the same filesystem, but the set of multiple CSV/JSON files is not one transaction. A process or machine crash between replacements can leave outputs from different runs; retain backups and rerun/rebuild before relying on them. For concurrent writers, large-scale production, or full transactional guarantees, use a database/warehouse with unique constraints and atomic transactions.
- Current state is rebuilt from the retained event history on each run. This favors correctness and simplicity over very large-history throughput.
- Event IDs are assumed globally unique. Conflicting reuse of an event ID is rejected rather than reconciled.
- This sample models only customer events and has no external parent/reference entity, so foreign-key validation is not applicable. If related entities are introduced, validate those keys against a reference table before accepting events.
- The implementation is single-process; distributed ordering, schema registry integration, and cloud orchestration are out of scope.
