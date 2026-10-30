
import pandas as pd
from pathlib import Path


def load_events(file_path):
    """Load change events from a CSV file."""
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    events = pd.read_csv(path)

    print(f"Successfully loaded {len(events)} events.")
    return events


if __name__ == "__main__":
    print("Change Data Pipeline - Ingestion Module")
