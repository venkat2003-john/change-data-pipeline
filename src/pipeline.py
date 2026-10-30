"""Deterministic CSV CDC pipeline for customer INSERT/UPDATE/DELETE events."""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = ["event_id", "customer_id", "event_type", "event_timestamp", "name", "email"]
CURRENT_COLUMNS = ["customer_id", "name", "email", "last_event_id", "last_event_timestamp"]
EVENT_TYPES = {"INSERT", "UPDATE", "DELETE"}
OUTPUT_FILES = ["event_history.csv", "current_state.csv", "rejected_events.csv", "reconciliation.json", "run_report.json"]


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _reason(row: pd.Series) -> str:
    reasons = []
    if not str(row.get("event_id", "")).strip(): reasons.append("missing_event_id")
    if not str(row.get("customer_id", "")).strip(): reasons.append("missing_customer_id")
    if str(row.get("event_type", "")).strip().upper() not in EVENT_TYPES: reasons.append("invalid_event_type")
    if pd.isna(row.get("_parsed_timestamp")): reasons.append("invalid_event_timestamp")
    return ";".join(reasons)


def _build_current_state(history: pd.DataFrame) -> pd.DataFrame:
    current = {}
    if history.empty:
        return pd.DataFrame(columns=CURRENT_COLUMNS)
    ordered = history.sort_values(["_parsed_timestamp", "event_id"], kind="mergesort")
    for _, event in ordered.iterrows():
        cid = event["customer_id"]
        if event["event_type"] == "DELETE":
            current.pop(cid, None)
        else:
            current[cid] = {"customer_id": cid, "name": event["name"], "email": event["email"],
                            "last_event_id": event["event_id"], "last_event_timestamp": event["event_timestamp"]}
    return pd.DataFrame(list(current.values()), columns=CURRENT_COLUMNS)


def _publish(output_dir: Path, files: dict[str, object]) -> None:
    """Stage complete files before replacing outputs; each replacement is atomic on the same filesystem."""
    staged = {}
    try:
        for name, content in files.items():
            fd, temp_name = tempfile.mkstemp(prefix=f".{name}.", suffix=".tmp", dir=output_dir)
            os.close(fd)
            temp_path = Path(temp_name)
            staged[name] = temp_path
            if isinstance(content, pd.DataFrame):
                content.to_csv(temp_path, index=False)
            else:
                temp_path.write_text(json.dumps(content, indent=2), encoding="utf-8")
        for name, temp_path in staged.items():
            os.replace(temp_path, output_dir / name)
    finally:
        for temp_path in staged.values():
            temp_path.unlink(missing_ok=True)


def process_events(input_file, output_dir, *, run_id=None):
    """Validate, deduplicate, append event history, reconcile and publish outputs."""
    started = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    run_id = run_id or str(uuid.uuid4())
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    input_file = Path(input_file)

    try:
        events = _read_csv(input_file)
        missing = sorted(set(REQUIRED_COLUMNS) - set(events.columns))
        unexpected = sorted(set(events.columns) - set(REQUIRED_COLUMNS))
        if missing or unexpected:
            details = []
            if missing: details.append(f"Missing columns: {missing}")
            if unexpected: details.append(f"unexpected columns: {unexpected}")
            raise ValueError("Schema validation failed (" + "; ".join(details) + "). Update the data contract before processing.")

        events = events[REQUIRED_COLUMNS].copy()
        events["event_type"] = events["event_type"].str.strip().str.upper()
        events["_parsed_timestamp"] = pd.to_datetime(events["event_timestamp"], errors="coerce", utc=True)
        invalid_mask = (events["event_id"].str.strip().eq("") | events["customer_id"].str.strip().eq("") |
                        ~events["event_type"].isin(EVENT_TYPES) | events["_parsed_timestamp"].isna())
        rejects = events.loc[invalid_mask].copy()
        if not rejects.empty:
            rejects["rejection_reason"] = rejects.apply(_reason, axis=1)

        accepted = events.loc[~invalid_mask].copy()
        duplicate_mask = accepted.duplicated(subset=["event_id"], keep="first")
        duplicate_rejects = accepted.loc[duplicate_mask].copy()
        if not duplicate_rejects.empty:
            duplicate_rejects["rejection_reason"] = "duplicate_event_id_in_batch"
            rejects = pd.concat([rejects, duplicate_rejects], ignore_index=True)
        accepted = accepted.loc[~duplicate_mask].copy()

        history_path = output_dir / "event_history.csv"
        if history_path.exists() and history_path.stat().st_size:
            prior = _read_csv(history_path)
            missing_history = sorted(set(REQUIRED_COLUMNS) - set(prior.columns))
            if missing_history:
                raise ValueError(f"Existing event history has invalid schema; missing columns: {missing_history}")
            prior = prior[REQUIRED_COLUMNS].copy()
            prior["_parsed_timestamp"] = pd.to_datetime(prior["event_timestamp"], errors="coerce", utc=True)
            if prior["_parsed_timestamp"].isna().any() or prior["event_id"].duplicated().any():
                raise ValueError("Existing event history failed integrity validation; restore a valid checkpoint before retrying.")
        else:
            prior = pd.DataFrame(columns=REQUIRED_COLUMNS + ["_parsed_timestamp"])

        known_ids = set(prior.get("event_id", pd.Series(dtype=str)).astype(str))
        replay_mask = accepted["event_id"].isin(known_ids)
        replay_rejects = accepted.loc[replay_mask].copy()
        if not replay_rejects.empty:
            replay_rejects["rejection_reason"] = "event_id_already_processed"
            rejects = pd.concat([rejects, replay_rejects], ignore_index=True)
        accepted = accepted.loc[~replay_mask].copy()

        new_history = accepted[REQUIRED_COLUMNS + ["_parsed_timestamp"]].copy()
        all_history = new_history.copy() if prior.empty else (prior.copy() if new_history.empty else pd.concat([prior, new_history], ignore_index=True))
        if not all_history.empty:
            all_history = all_history.sort_values(["_parsed_timestamp", "event_id"], kind="mergesort")
            all_history["event_timestamp"] = all_history["_parsed_timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        history_out = all_history[REQUIRED_COLUMNS].copy()
        current = _build_current_state(all_history)
        reject_columns = REQUIRED_COLUMNS + ["rejection_reason"]
        if rejects.empty:
            rejects_out = pd.DataFrame(columns=reject_columns)
        else:
            rejects_out = rejects.copy()
            rejects_out["event_timestamp"] = rejects_out["event_timestamp"].astype(str)
            rejects_out = rejects_out[reject_columns]

        # Reconciliation assertions: accepted history is unique and current state is reproducible.
        duplicate_history_ids = int(history_out["event_id"].duplicated().sum())
        rebuilt_current = _build_current_state(all_history)
        current_matches_rebuild = current.equals(rebuilt_current)
        reconciliation = {
            "run_id": run_id,
            "input_rows": int(len(events)),
            "accepted_new_events": int(len(accepted)),
            "rejected_rows": int(len(rejects_out)),
            "total_history_rows": int(len(history_out)),
            "unique_history_event_ids": int(history_out["event_id"].nunique()),
            "duplicate_history_event_ids": duplicate_history_ids,
            "current_customer_rows": int(len(current)),
            "current_state_matches_history_replay": bool(current_matches_rebuild),
            "history_count_reconciles": bool(len(history_out) == len(prior) + len(accepted)),
            "status": "passed" if duplicate_history_ids == 0 and current_matches_rebuild and len(history_out) == len(prior) + len(accepted) else "failed",
        }
        if reconciliation["status"] != "passed":
            raise RuntimeError("Reconciliation failed; outputs were not published.")

        elapsed = time.perf_counter() - started
        ended_at = datetime.now(timezone.utc)
        report = {"run_id": run_id, "status": "success", "started_at": started_at.isoformat(),
                  "ended_at": ended_at.isoformat(), "duration_seconds": round(elapsed, 6),
                  "source_file": input_file.name, "input_rows": int(len(events)),
                  "accepted_new_events": int(len(accepted)), "rejected_rows": int(len(rejects_out)),
                  "total_history_rows": int(len(history_out)), "current_customer_rows": int(len(current)),
                  "reconciliation_status": reconciliation["status"]}
        _publish(output_dir, {"event_history.csv": history_out, "current_state.csv": current,
                              "rejected_events.csv": rejects_out, "reconciliation.json": reconciliation,
                              "run_report.json": report})
        logging.info("CDC run completed: %s", report)
        print(f"Run {run_id}: input={len(events)}, accepted_new={len(accepted)}, rejected={len(rejects_out)}, history={len(history_out)}, current={len(current)}, duration={elapsed:.3f}s")
        return report
    except Exception as exc:
        failure_report = {"run_id": run_id, "status": "failed", "started_at": started_at.isoformat(),
                          "ended_at": datetime.now(timezone.utc).isoformat(), "source_file": input_file.name,
                          "error_type": type(exc).__name__, "error_message": str(exc)}
        # Failure status is best-effort; existing data outputs are left untouched when validation fails.
        try:
            (output_dir / "run_report.json").write_text(json.dumps(failure_report, indent=2), encoding="utf-8")
        except OSError:
            logging.exception("Could not persist failed-run report")
        raise


if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    process_events(root / "data" / "sample_events.csv", root / "outputs")
