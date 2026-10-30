
import time
import pandas as pd
from pathlib import Path
from src.pipeline import process_events


def test_pipeline_performance(tmp_path):
    input_file = tmp_path / "large_events.csv"
    output_dir = tmp_path / "outputs"

    # Generate 10,000 sample events
    data = []

    for i in range(10000):
        data.append({
            "event_id": f"evt{i}",
            "customer_id": f"C{i}",
            "event_type": "INSERT",
            "event_timestamp": "2026-10-01T09:00:00+00:00",
            "name": f"Customer{i}",
            "email": f"customer{i}@example.com"
        })

    pd.DataFrame(data).to_csv(input_file, index=False)

    start = time.perf_counter()
    process_events(input_file, output_dir)
    elapsed = time.perf_counter() - start

    print(f"Processed 10,000 events in {elapsed:.2f} seconds")

    assert elapsed < 30
    assert (output_dir / "current_state.csv").exists()
    assert (output_dir / "event_history.csv").exists()
    assert (output_dir / "rejected_events.csv").exists()
