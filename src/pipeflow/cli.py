"""CLI interface for pipeflow using argparse."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Sequence

from pipeflow.config import load_config
from pipeflow.pipeline import Pipeline


def main(argv: Sequence[str] | None = None) -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="pipeflow",
        description="pipeflow — A modular ETL pipeline framework",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # run command
    run_parser = subparsers.add_parser("run", help="Execute a pipeline from a YAML config")
    run_parser.add_argument("config", help="Path to pipeline YAML config file")

    # validate command
    validate_parser = subparsers.add_parser("validate", help="Validate a pipeline config")
    validate_parser.add_argument("config", help="Path to pipeline YAML config file")

    # inspect command
    inspect_parser = subparsers.add_parser("inspect", help="Inspect a data file")
    inspect_parser.add_argument("file", help="Path to CSV or JSON file to inspect")
    inspect_parser.add_argument(
        "-n", "--rows", type=int, default=5, help="Number of sample rows to show"
    )

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 1

    match args.command:
        case "run":
            return _cmd_run(args.config)
        case "validate":
            return _cmd_validate(args.config)
        case "inspect":
            return _cmd_inspect(args.file, args.rows)
        case _:
            parser.print_help()
            return 1


def _cmd_run(config_path: str) -> int:
    """Execute a pipeline."""
    try:
        config = load_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error loading config: {e}", file=sys.stderr)
        return 1

    try:
        pipeline = Pipeline(config)
        metrics = pipeline.run()
        print(json.dumps(metrics, indent=2))
        return 0
    except Exception as e:
        print(f"Pipeline error: {e}", file=sys.stderr)
        return 1


def _cmd_validate(config_path: str) -> int:
    """Validate a pipeline config file."""
    try:
        config = load_config(config_path)
        print(f"✓ Config '{config.name}' is valid")
        print(f"  Extract: {config.extract.type}")
        print(f"  Transforms: {len(config.transforms)}")
        print(f"  Validation: {'yes' if config.validate else 'no'}")
        print(f"  Load: {config.load.type}")
        return 0
    except (FileNotFoundError, ValueError) as e:
        print(f"✗ Config error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"✗ Validation error: {e}", file=sys.stderr)
        return 1


def _cmd_inspect(file_path: str, num_rows: int) -> int:
    """Inspect a data file: show schema, row count, sample rows."""
    path = Path(file_path)
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        return 1

    suffix = path.suffix.lower()
    try:
        if suffix == ".csv":
            return _inspect_csv(path, num_rows)
        elif suffix in (".json", ".jsonl"):
            return _inspect_json(path, num_rows, jsonl=(suffix == ".jsonl"))
        else:
            print(f"Unsupported file type: {suffix}", file=sys.stderr)
            return 1
    except Exception as e:
        print(f"Inspect error: {e}", file=sys.stderr)
        return 1


def _inspect_csv(path: Path, num_rows: int) -> int:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for i, row in enumerate(reader):
            rows.append(dict(row))
            if i + 1 >= num_rows:
                break
        # Count remaining
        count = len(rows)
        for _ in reader:
            count += 1

    fieldnames = list(rows[0].keys()) if rows else []
    print(f"File: {path}")
    print(f"Format: CSV")
    print(f"Columns: {fieldnames}")
    print(f"Total rows: {count}")
    print(f"\nSample ({min(num_rows, len(rows))} rows):")
    for row in rows:
        print(f"  {row}")
    return 0


def _inspect_json(path: Path, num_rows: int, jsonl: bool) -> int:
    records: list[dict] = []  # type: ignore[type-arg]
    if jsonl:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    else:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = [data]

    fieldnames = list(records[0].keys()) if records else []
    print(f"File: {path}")
    print(f"Format: {'JSONL' if jsonl else 'JSON'}")
    print(f"Fields: {fieldnames}")
    print(f"Total records: {len(records)}")
    print(f"\nSample ({min(num_rows, len(records))} records):")
    for rec in records[:num_rows]:
        print(f"  {rec}")
    return 0
