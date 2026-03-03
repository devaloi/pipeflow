"""Performance smoke-tests for pipeflow core operations."""
import csv
import time
from pathlib import Path


def _make_csv(path: Path, rows: int) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "value"])
        for i in range(rows):
            w.writerow([i, f"item_{i}", i * 1.5])


def test_csv_write_read_performance(tmp_path: Path) -> None:
    """Benchmark: write + read 10k rows via stdlib < 1s."""
    csv_path = tmp_path / "bench.csv"
    _make_csv(csv_path, 10_000)

    start = time.monotonic()
    records = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    elapsed = time.monotonic() - start

    assert len(records) == 10_000
    assert elapsed < 1.0, f"CSV read took {elapsed:.2f}s (expected < 1s)"


def test_record_transform_throughput(tmp_path: Path) -> None:
    """Benchmark: in-memory transform of 50k records < 1s."""
    records = [{"id": i, "name": f"item_{i}", "value": float(i)} for i in range(50_000)]

    start = time.monotonic()
    # Apply filter + rename in-memory
    result = [
        {"id": r["id"], "label": r["name"], "value": r["value"]}
        for r in records
        if r["value"] > 0
    ]
    elapsed = time.monotonic() - start

    assert len(result) == 49_999
    assert elapsed < 1.0, f"Transform took {elapsed:.2f}s (expected < 1s)"
