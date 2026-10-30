import pandas as pd
from src.generate_events import generate_events


def test_generator_is_reproducible(tmp_path):
    a = generate_events(tmp_path / "a.csv", event_count=100, seed=7)
    b = generate_events(tmp_path / "b.csv", event_count=100, seed=7)
    pd.testing.assert_frame_equal(pd.read_csv(a), pd.read_csv(b))


def test_generator_supports_large_configurable_batch(tmp_path):
    path = generate_events(tmp_path / "large.csv", event_count=100_000, seed=42)
    frame = pd.read_csv(path)
    assert len(frame) == 100_000
    assert frame["event_id"].is_unique
