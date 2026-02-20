# pipeflow

[![CI](https://github.com/devaloi/pipeflow/actions/workflows/ci.yml/badge.svg)](https://github.com/devaloi/pipeflow/actions/workflows/ci.yml)

A modular ETL pipeline framework in Python — ingest CSV/JSON/API sources, transform with composable steps, load to SQLite or CSV with validation and error handling.

## Features

- **Multiple extractors** — CSV, JSON/JSONL files, HTTP APIs with pagination
- **Composable transforms** — rename, cast, filter, derive, deduplicate
- **Schema validation** — Pydantic models validate every record; bad records go to error log
- **Multiple loaders** — SQLite (with upsert) and CSV output
- **YAML configuration** — declare pipelines as config, not code
- **Observable** — structured JSON logging, record counts, timing, error rates
- **Streaming** — extractors yield records as generators, not in-memory lists
- **Minimal dependencies** — only PyYAML + Pydantic required

## Quick Start

```bash
# Install
pip install -e .

# Run the CSV → SQLite example
python -m pipeflow run examples/csv_to_sqlite/pipeline.yaml

# Validate a config file
python -m pipeflow validate examples/csv_to_sqlite/pipeline.yaml

# Inspect a data file
python -m pipeflow inspect examples/csv_to_sqlite/data.csv
```

## Pipeline Configuration

Pipelines are defined in YAML with four stages: **extract → transform → validate → load**.

```yaml
name: users_import
extract:
  type: csv
  path: ./data/users.csv
  delimiter: ","

transforms:
  - type: rename
    mapping: { "Full Name": "name" }
  - type: cast
    columns: { age: int, created_at: datetime }
  - type: filter
    condition: "age >= 18"
  - type: deduplicate
    key: email

validate:
  model: UserRecord
  fields:
    name: { type: str }
    email: { type: str }
    age: { type: int }

load:
  type: sqlite
  database: ./output.db
  table: users
  mode: upsert
  conflict_key: email
```

## Config Reference

### Extract

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `str` | *required* | `csv`, `json`, `jsonl`, or `api` |
| `path` | `str` | — | File path (for csv/json/jsonl) |
| `url` | `str` | — | API endpoint URL (for api type) |
| `delimiter` | `str` | `","` | CSV delimiter |
| `encoding` | `str` | `"utf-8"` | File encoding |
| `headers` | `dict` | `{}` | HTTP headers (for api type) |
| `params` | `dict` | `{}` | Query parameters (for api type) |
| `pagination` | `dict` | `null` | Pagination config: `{type: offset, limit: 100}` |

### Transforms

| Type | Fields | Description |
|------|--------|-------------|
| `rename` | `mapping: {old: new}` | Rename column keys |
| `cast` | `columns: {col: type}` | Cast types: `int`, `float`, `str`, `bool`, `datetime` |
| `filter` | `condition: "expr"` | Keep rows where expression is true |
| `derive` | `expression: "new = expr"` | Compute new column from expression |
| `deduplicate` | `key: col` or `key: [col1, col2]` | Remove duplicates by key |

### Validate

| Field | Type | Description |
|-------|------|-------------|
| `model` | `str` | Model name (for logging) |
| `fields` | `dict` | Field definitions: `{name: {type: str, required: true}}` |

### Load

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `str` | *required* | `sqlite` or `csv` |
| `database` | `str` | — | SQLite database path |
| `table` | `str` | — | Table name (for sqlite) |
| `path` | `str` | — | Output file path (for csv) |
| `mode` | `str` | `"insert"` | `insert` or `upsert` |
| `conflict_key` | `str` | — | Column for upsert conflict resolution |
| `batch_size` | `int` | `100` | Records per batch insert |

## CLI Commands

```
python -m pipeflow run <config.yaml>       # Execute a pipeline
python -m pipeflow validate <config.yaml>  # Validate config syntax
python -m pipeflow inspect <data_file>     # Show schema, row count, samples
```

## Architecture

```
src/pipeflow/
├── cli.py               # argparse CLI
├── config.py            # YAML config → Pydantic models
├── types.py             # Shared type aliases (Record)
├── pipeline.py          # Pipeline orchestrator
├── lib/                 # Shared utilities
│   ├── safe_eval.py     # Restricted expression evaluator (no eval())
│   └── types.py         # Shared type/cast mappings
├── extractors/          # CSV, JSON, API extractors (Protocol-based)
├── transforms/          # Rename, cast, filter, derive, deduplicate
├── loaders/             # SQLite (upsert), CSV writer
├── validation/          # Dynamic Pydantic model validation
└── observability/       # Structured logging + metrics
```

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
python -m pytest -v

# Lint
python -m ruff check src/ tests/

# Type check
python -m mypy src/pipeflow/
```

## License

MIT — Copyright (c) 2026 Jason Aloi
