"""Generate reproducible synthetic CDC events for scale/performance testing."""
import argparse
from pathlib import Path
import numpy as np
import pandas as pd


def generate_events(output_file, event_count=100_000, seed=42, customer_count=None):
    if event_count < 0:
        raise ValueError("event_count must be >= 0")
    customer_count = customer_count or max(1, event_count // 3)
    rng = np.random.default_rng(seed)
    ids = np.arange(event_count)
    customer_ids = rng.integers(0, customer_count, size=event_count)
    types = rng.choice(["INSERT", "UPDATE", "DELETE"], size=event_count, p=[0.45, 0.45, 0.10])
    base = pd.Timestamp("2026-01-01T00:00:00Z")
    seconds = rng.integers(0, 365 * 24 * 3600, size=event_count)
    frame = pd.DataFrame({
        "event_id": [f"evt_{i:09d}" for i in ids],
        "customer_id": [f"C{value:08d}" for value in customer_ids],
        "event_type": types,
        "event_timestamp": (base + pd.to_timedelta(seconds, unit="s")).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "name": [f"Customer {value:08d}" for value in customer_ids],
        "email": [f"customer{value:08d}@example.com" for value in customer_ids],
    })
    path = Path(output_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="data/generated_events.csv")
    args = parser.parse_args()
    print(generate_events(args.output, args.rows, args.seed))
