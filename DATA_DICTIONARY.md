# Data Dictionary

## Input: customer event CSV

| Field | Type / format | Required | Meaning / rule |
|---|---|---:|---|
| `event_id` | string | Yes | Globally unique event identifier; duplicates are rejected. |
| `customer_id` | string | Yes | Customer/entity key affected by the event. |
| `event_type` | string | Yes | `INSERT`, `UPDATE`, or `DELETE` (case-normalized). |
| `event_timestamp` | ISO-8601 timestamp | Yes | Event ordering timestamp; parsed and normalized to UTC. |
| `name` | string | Yes | Customer name attribute; may be blank for DELETE. |
| `email` | string | Yes | Customer email attribute; may be blank for DELETE. |

## Outputs

### `event_history.csv`
Append-only accepted input events with the six input fields. Rows are sorted by `event_timestamp`, then `event_id`. This is the audit/replay source for current state.

### `current_state.csv`
| Field | Meaning |
|---|---|
| `customer_id` | Unique customer key. |
| `name` | Latest accepted name. |
| `email` | Latest accepted email. |
| `last_event_id` | Event that established the current state. |
| `last_event_timestamp` | UTC timestamp of that event. |

Deleted customers are absent from this file.

### `rejected_events.csv`
Contains the six input fields plus `rejection_reason`. Current reason codes include `missing_event_id`, `missing_customer_id`, `invalid_event_type`, `invalid_event_timestamp`, `duplicate_event_id_in_batch`, and `event_id_already_processed`.

### `run_report.json`
Run metadata: `run_id`, `status`, `started_at`, `ended_at`, `duration_seconds`, `source_file`, `input_rows`, `accepted_new_events`, `rejected_rows`, `total_history_rows`, and `current_customer_rows`.

## Assumptions

- `event_id` is globally unique across all batches.
- Timestamps are authoritative for event order; `event_id` is a deterministic tie-breaker.
- INSERT and UPDATE both upsert the supplied full customer attributes.
- DELETE is a tombstone: it removes current state but stays in history.
- A blank `name` or `email` is allowed by this sample contract; required validation is focused on event identity, customer identity, event type, and timestamp.
