from pathlib import Path
import pandas as pd

from src.pipeline import process_events


def test_pipeline_outputs(tmp_path):
    input_file = tmp_path / "events.csv"

    data = {
        "event_id": ["evt001"],
        "customer_id": ["C001"],
        "event_type": ["INSERT"],
        "event_timestamp": ["2026-10-01 10:00:00"],
        "name": ["Rahul"],
        "email": ["rahul@example.com"],
    }

    pd.DataFrame(data).to_csv(input_file, index=False)

    process_events(input_file, tmp_path)

    assert (tmp_path / "event_history.csv").exists()
    assert (tmp_path / "current_state.csv").exists()
    assert (tmp_path / "rejected_events.csv").exists()
    
def test_invalid_event_is_rejected(tmp_path):
    input_file = tmp_path / "events.csv"

    data = {
        "event_id": [""],
        "customer_id": ["C002"],
        "event_type": ["INSERT"],
        "event_timestamp": ["2026-10-01 11:00:00"],
        "name": ["Arun"],
        "email": ["arun@example.com"],
    }

    pd.DataFrame(data).to_csv(input_file, index=False)

    process_events(input_file, tmp_path)

    rejected = pd.read_csv(tmp_path / "rejected_events.csv")

    assert len(rejected) == 1
  
def test_duplicate_event_is_rejected(tmp_path):
    import pandas as pd
    from pathlib import Path
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from pipeline import process_events

    input_file = tmp_path / "input.csv"
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()

    data = {
        "event_id": ["evt001", "evt001"],
        "customer_id": ["C001", "C001"],
        "event_type": ["INSERT", "INSERT"],
        "event_timestamp": [
            "2026-10-01 10:00:00",
            "2026-10-01 10:00:00"
        ],
        "name": ["Rahul", "Rahul"],
        "email": ["rahul@example.com", "rahul@example.com"]
    }

    pd.DataFrame(data).to_csv(input_file, index=False)
    process_events(input_file, output_dir)

    rejected = pd.read_csv(output_dir / "rejected_events.csv")
    assert len(rejected) == 1

def test_insert_creates_customer(tmp_path):
    import pandas as pd
    from pathlib import Path
    import sys

    sys.path.insert(
        0, str(Path(__file__).resolve().parents[1] / "src")
    )
    from pipeline import process_events

    input_file = tmp_path / "input.csv"
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()

    data = {
        "event_id": ["evt100"],
        "customer_id": ["C100"],
        "event_type": ["INSERT"],
        "event_timestamp": ["2026-10-01 10:00:00"],
        "name": ["Ravi"],
        "email": ["ravi@example.com"]
    }

    pd.DataFrame(data).to_csv(input_file, index=False)
    process_events(input_file, output_dir)

    current = pd.read_csv(output_dir / "current_state.csv")

    assert len(current) == 1
    assert current.iloc[0]["customer_id"] == "C100"
    assert current.iloc[0]["name"] == "Ravi"

def test_update_changes_customer(tmp_path):
    import pandas as pd
    from pathlib import Path
    import sys

    sys.path.insert(
        0, str(Path(__file__).resolve().parents[1] / "src")
    )
    from pipeline import process_events

    input_file = tmp_path / "input.csv"
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()

    data = {
        "event_id": ["evt101", "evt102"],
        "customer_id": ["C101", "C101"],
        "event_type": ["INSERT", "UPDATE"],
        "event_timestamp": [
            "2026-10-01 10:00:00",
            "2026-10-01 11:00:00"
        ],
        "name": ["Ravi", "Ravi Kumar"],
        "email": [
            "ravi@example.com",
            "ravi.kumar@example.com"
        ]
    }

    pd.DataFrame(data).to_csv(input_file, index=False)
    process_events(input_file, output_dir)

    current = pd.read_csv(output_dir / "current_state.csv")

    assert len(current) == 1
    assert current.iloc[0]["customer_id"] == "C101"
    assert current.iloc[0]["name"] == "Ravi Kumar"
    assert current.iloc[0]["email"] == "ravi.kumar@example.com"

def test_out_of_order_update_does_not_overwrite_newer_customer(tmp_path):
    import pandas as pd
    from pathlib import Path
    from src.pipeline import process_events

    input_file = tmp_path / "out_of_order.csv"
    output_dir = tmp_path / "outputs"

    data = {
        "event_id": ["evt_new", "evt_old"],
        "customer_id": ["C100", "C100"],
        "event_type": ["INSERT", "UPDATE"],
        "event_timestamp": [
            "2026-10-03T10:00:00Z",
            "2026-10-02T10:00:00Z",
        ],
        "name": ["Ravi Kumar", "Old Name"],
        "email": ["ravi@example.com", "old@example.com"],
    }

    pd.DataFrame(data).to_csv(input_file, index=False)
    process_events(input_file, output_dir)

    current = pd.read_csv(output_dir / "current_state.csv")

    assert len(current) == 1
    assert current.iloc[0]["customer_id"] == "C100"
    assert current.iloc[0]["name"] == "Ravi Kumar"

def test_rerun_is_idempotent(tmp_path):
    import pandas as pd
    from pathlib import Path
    from src.pipeline import process_events

    project_root = Path(__file__).resolve().parent.parent
    input_file = project_root / "data" / "sample_events.csv"
    output_dir = tmp_path / "outputs"

    # First run
    process_events(input_file, output_dir)

    current_first = pd.read_csv(
        output_dir / "current_state.csv"
    )
    history_first = pd.read_csv(
        output_dir / "event_history.csv"
    )

    # Second run using the same input
    process_events(input_file, output_dir)

    current_second = pd.read_csv(
        output_dir / "current_state.csv"
    )
    history_second = pd.read_csv(
        output_dir / "event_history.csv"
    )

    # Outputs should remain unchanged
    pd.testing.assert_frame_equal(
        current_first, current_second
    )
    pd.testing.assert_frame_equal(
        history_first, history_second
    )

def test_empty_input_file_is_handled(tmp_path):
    input_file = tmp_path / "empty_events.csv"
    output_dir = tmp_path / "outputs"

    # Create a CSV with the required headers but no data rows
    input_file.write_text(
        "event_id,customer_id,event_type,event_timestamp,name,email\n",
        encoding="utf-8"
    )

    process_events(input_file, output_dir)

    current = pd.read_csv(output_dir / "current_state.csv")
    history = pd.read_csv(output_dir / "event_history.csv")

    assert len(current) == 0
    assert len(history) == 0

def test_delete_removes_customer_and_preserves_history(tmp_path):
    input_file = tmp_path / "delete_events.csv"
    output_dir = tmp_path / "outputs"

    data = {
        "event_id": ["evt_insert", "evt_delete"],
        "customer_id": ["C200", "C200"],
        "event_type": ["INSERT", "DELETE"],
        "event_timestamp": [
            "2026-10-01 10:00:00",
            "2026-10-01 11:00:00",
        ],
        "name": ["Ravi", "Ravi"],
        "email": ["ravi@example.com", "ravi@example.com"],
    }

    pd.DataFrame(data).to_csv(input_file, index=False)
    process_events(input_file, output_dir)

    current = pd.read_csv(output_dir / "current_state.csv")
    history = pd.read_csv(output_dir / "event_history.csv")

    assert "C200" not in current["customer_id"].values
    assert len(history) == 2
    assert set(history["event_type"]) == {"INSERT", "DELETE"}

    
def test_old_insert_does_not_restore_customer_after_newer_delete(tmp_path):
    input_file = tmp_path / "late_events.csv"
    output_dir = tmp_path / "outputs"

    data = {
        "event_id": ["evt_delete", "evt_old_insert"],
        "customer_id": ["C300", "C300"],
        "event_type": ["DELETE", "INSERT"],
        "event_timestamp": [
            "2026-10-03 10:00:00",
            "2026-10-01 10:00:00",
        ],
        "name": ["Ravi", "Ravi"],
        "email": ["ravi@example.com", "ravi@example.com"],
    }

    pd.DataFrame(data).to_csv(input_file, index=False)
    process_events(input_file, output_dir)

    current = pd.read_csv(output_dir / "current_state.csv")

    assert "C300" not in current["customer_id"].values



def test_event_id_replay_across_separate_batches_is_rejected(tmp_path):
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    out = tmp_path / "out"
    row = {
        "event_id": "same-id", "customer_id": "C900", "event_type": "INSERT",
        "event_timestamp": "2026-10-01T10:00:00Z", "name": "First", "email": "first@example.com"
    }
    pd.DataFrame([row]).to_csv(first, index=False)
    pd.DataFrame([row]).to_csv(second, index=False)
    process_events(first, out)
    process_events(second, out)
    history = pd.read_csv(out / "event_history.csv")
    rejected = pd.read_csv(out / "rejected_events.csv")
    assert len(history) == 1
    assert rejected.iloc[0]["rejection_reason"] == "event_id_already_processed"


def test_invalid_type_and_timestamp_have_rejection_reasons(tmp_path):
    path = tmp_path / "bad.csv"
    pd.DataFrame([
        {"event_id": "e1", "customer_id": "C1", "event_type": "UPSERT", "event_timestamp": "2026-01-01", "name": "A", "email": "a@x.com"},
        {"event_id": "e2", "customer_id": "C2", "event_type": "INSERT", "event_timestamp": "not-a-date", "name": "B", "email": "b@x.com"},
    ]).to_csv(path, index=False)
    process_events(path, tmp_path / "out")
    rejected = pd.read_csv(tmp_path / "out" / "rejected_events.csv")
    assert set(rejected["rejection_reason"]) == {"invalid_event_type", "invalid_event_timestamp"}


def test_missing_required_column_fails_clearly(tmp_path):
    path = tmp_path / "bad_schema.csv"
    pd.DataFrame([{"event_id": "e1"}]).to_csv(path, index=False)
    import pytest
    with pytest.raises(ValueError, match="Missing columns"):
        process_events(path, tmp_path / "out")


def test_run_report_contains_counts_and_run_id(tmp_path):
    path = tmp_path / "one.csv"
    pd.DataFrame([{
        "event_id": "e1", "customer_id": "C1", "event_type": "INSERT",
        "event_timestamp": "2026-01-01T00:00:00Z", "name": "A", "email": "a@x.com"
    }]).to_csv(path, index=False)
    report = process_events(path, tmp_path / "out", run_id="test-run-001")
    assert report["run_id"] == "test-run-001"
    assert report["input_rows"] == 1
    assert report["accepted_new_events"] == 1
    assert report["current_customer_rows"] == 1
    assert (tmp_path / "out" / "run_report.json").exists()


def test_unexpected_schema_column_fails_and_records_failed_run(tmp_path):
    import pytest
    path = tmp_path / "schema_drift.csv"
    pd.DataFrame([{
        "event_id": "e1", "customer_id": "C1", "event_type": "INSERT",
        "event_timestamp": "2026-10-01T10:00:00Z", "name": "A",
        "email": "a@example.com", "unexpected_field": "new-column",
    }]).to_csv(path, index=False)
    out = tmp_path / "out"
    with pytest.raises(ValueError, match="unexpected columns"):
        process_events(path, out, run_id="schema-drift-test")
    import json
    report = json.loads((out / "run_report.json").read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["error_type"] == "ValueError"


def test_reconciliation_report_passes_and_counts_match(tmp_path):
    import json
    path = tmp_path / "events.csv"
    pd.DataFrame([
        {"event_id": "e1", "customer_id": "C1", "event_type": "INSERT", "event_timestamp": "2026-10-01T10:00:00Z", "name": "A", "email": "a@example.com"},
        {"event_id": "e2", "customer_id": "C1", "event_type": "UPDATE", "event_timestamp": "2026-10-01T11:00:00Z", "name": "B", "email": "b@example.com"},
    ]).to_csv(path, index=False)
    out = tmp_path / "out"
    report = process_events(path, out, run_id="reconcile-test")
    reconciliation = json.loads((out / "reconciliation.json").read_text(encoding="utf-8"))
    assert report["reconciliation_status"] == "passed"
    assert reconciliation["status"] == "passed"
    assert reconciliation["input_rows"] == reconciliation["accepted_new_events"] + reconciliation["rejected_rows"]
    assert reconciliation["total_history_rows"] == reconciliation["unique_history_event_ids"] == 2
    assert reconciliation["current_customer_rows"] == 1
    assert reconciliation["current_state_matches_history_replay"] is True
