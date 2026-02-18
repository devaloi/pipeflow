# P03: pipeflow — Python ETL Pipeline

**Catalog ID:** P03 | **Size:** M | **Language:** Python
**Repo name:** `pipeflow`
**One-liner:** A modular ETL pipeline framework in Python — ingest CSV/JSON/API sources, transform with composable steps, load to SQLite/PostgreSQL with validation and error handling.

---

## Why This Stands Out

- **Real data engineering patterns** — extract → transform → load with proper error handling
- **Composable transforms** — chain small functions, not monolithic scripts
- **Multiple sources** — CSV, JSON files, HTTP API endpoints
- **Multiple sinks** — SQLite and PostgreSQL with upsert support
- **Schema validation** — Pydantic models validate every record, bad records go to error log
- **Async HTTP ingestion** — httpx for concurrent API fetching
- **Observable** — structured logging, record counts, timing at each stage
- **CLI interface** — run pipelines from command line with config files

---

## Architecture

```
pipeflow/
├── src/
│   └── pipeflow/
│       ├── __init__.py
│       ├── __main__.py           # CLI entry point
│       ├── cli.py                # Click CLI: run, validate, inspect
│       ├── pipeline.py           # Pipeline class: chain extract → transform → load
│       ├── config.py             # Pipeline config from YAML
│       ├── extractors/
│       │   ├── __init__.py
│       │   ├── base.py           # Extractor protocol
│       │   ├── csv.py            # CSV file reader (streaming, configurable delimiter)
│       │   ├── json.py           # JSON/JSONL file reader
│       │   ├── api.py            # HTTP API extractor (async, pagination)
│       │   └── extractors_test.py
│       ├── transforms/
│       │   ├── __init__.py
│       │   ├── base.py           # Transform protocol
│       │   ├── rename.py         # Rename columns
│       │   ├── filter.py         # Filter rows by predicate
│       │   ├── cast.py           # Type casting (str→int, date parsing, etc.)
│       │   ├── derive.py         # Compute new columns from expressions
│       │   ├── deduplicate.py    # Remove duplicates by key
│       │   └── transforms_test.py
│       ├── loaders/
│       │   ├── __init__.py
│       │   ├── base.py           # Loader protocol
│       │   ├── sqlite.py         # SQLite loader with auto-create table
│       │   ├── postgres.py       # PostgreSQL loader with upsert
│       │   ├── csv_writer.py     # CSV output
│       │   └── loaders_test.py
│       ├── validation/
│       │   ├── __init__.py
│       │   ├── validator.py      # Pydantic model validation per record
│       │   └── validator_test.py
│       └── observability/
│           ├── __init__.py
│           ├── logger.py         # Structured logging (JSON format)
│           └── metrics.py        # Record counts, timing, error rates
├── tests/
│   ├── conftest.py               # Fixtures: sample files, temp DBs
│   ├── test_pipeline.py          # End-to-end pipeline tests
│   └── test_config.py            # Config loading tests
├── examples/
│   ├── csv_to_sqlite/
│   │   ├── pipeline.yaml         # Config file
│   │   └── data.csv              # Sample data
│   └── api_to_postgres/
│       └── pipeline.yaml
├── pyproject.toml
├── uv.lock
├── Makefile
├── .env.example
├── .gitignore
├── LICENSE
└── README.md
```

---

## Pipeline Config (YAML)

```yaml
name: users_import
extract:
  type: csv
  path: ./data/users.csv
  delimiter: ","

transforms:
  - type: rename
    mapping: { "Full Name": "name", "Email Address": "email" }
  - type: cast
    columns: { age: int, created_at: datetime }
  - type: filter
    condition: "age >= 18"
  - type: deduplicate
    key: email

validate:
  model: UserRecord  # Pydantic model name

load:
  type: sqlite
  database: ./output.db
  table: users
  mode: upsert
  conflict_key: email
```

---

## Tech Stack

| Component | Choice |
|-----------|--------|
| Language | Python 3.11+ |
| CLI | Click |
| Validation | Pydantic v2 |
| HTTP | httpx (async) |
| Database | sqlite3 (stdlib) + asyncpg (PostgreSQL) |
| Config | PyYAML |
| Testing | pytest + pytest-asyncio |
| Type checking | mypy (strict) |
| Linting | ruff |
| Package manager | uv |

---

## Phased Build Plan

### Phase 1: Foundation

**1.1 — Project setup**
- `uv init pipeflow`, pyproject.toml, dev deps
- Makefile: test, lint, typecheck, run

**1.2 — Pipeline core + config**
- `Pipeline` class: accepts extractor, transforms, validator, loader
- `run()` method: extract → transform chain → validate → load, return metrics
- YAML config parsing with Pydantic model
- Tests: pipeline with mock components

### Phase 2: Extractors

**2.1 — CSV extractor**
- Streaming reader (yield rows, don't load all into memory)
- Configurable: delimiter, encoding, skip rows, header mapping
- Tests: basic CSV, custom delimiter, missing columns, large file

**2.2 — JSON extractor**
- Support both JSON array and JSONL (line-delimited)
- Nested field flattening (dot notation: `user.name` → `user_name`)
- Tests: array, JSONL, nested

**2.3 — API extractor**
- Async HTTP with httpx
- Pagination support (offset, cursor, link header)
- Rate limiting (configurable requests/second)
- Tests: mock HTTP server, pagination, rate limit

### Phase 3: Transforms

**3.1 — Column transforms**
- `rename` — column name mapping
- `cast` — type conversion (int, float, datetime, boolean)
- `derive` — compute new columns from simple expressions
- Each transform is a function: `(record: dict) → dict`
- Chain: transforms composed left to right
- Tests: each transform, chain composition

**3.2 — Row transforms**
- `filter` — keep/discard rows by predicate
- `deduplicate` — remove duplicates by key column(s)
- Tests: filter conditions, dedup with various keys

### Phase 4: Validation + Loading

**4.1 — Pydantic validation**
- Validate each record against Pydantic model
- Valid records pass through, invalid go to error log with details
- Configurable: fail-fast vs collect-all-errors
- Tests: valid records pass, invalid rejected with error details

**4.2 — SQLite loader**
- Auto-create table from first record's schema
- Insert and upsert (ON CONFLICT) modes
- Batch insert (configurable batch size)
- Tests: create table, insert, upsert, batch

**4.3 — PostgreSQL loader**
- asyncpg connection
- Same interface as SQLite loader
- Upsert with ON CONFLICT DO UPDATE
- Tests: with temp PostgreSQL (or mock)

**4.4 — CSV writer**
- Output validated/transformed records to CSV
- Tests: round-trip CSV → transform → CSV

### Phase 5: CLI + Polish

**5.1 — CLI**
- `pipeflow run pipeline.yaml` — execute pipeline
- `pipeflow validate pipeline.yaml` — validate config without running
- `pipeflow inspect data.csv` — show schema, row count, sample
- Tests: CLI invocation

**5.2 — Observability**
- Structured JSON logging at each stage
- Metrics: records_extracted, records_transformed, records_valid, records_loaded, records_errored, duration_seconds
- Print summary on completion

**5.3 — Examples**
- `csv_to_sqlite/` — CSV import with transforms
- `api_to_postgres/` — API fetch with pagination

**5.4 — README**
- Badges, install, quick start
- Pipeline config reference
- Available extractors, transforms, loaders
- How to add custom components
- Example output

---

## Commit Plan

1. `chore: scaffold project with uv`
2. `feat: add pipeline core and YAML config`
3. `feat: add CSV and JSON extractors`
4. `feat: add API extractor with async pagination`
5. `feat: add column transforms (rename, cast, derive)`
6. `feat: add row transforms (filter, deduplicate)`
7. `feat: add Pydantic record validation`
8. `feat: add SQLite loader with upsert`
9. `feat: add PostgreSQL loader`
10. `feat: add CLI with Click`
11. `feat: add observability (logging, metrics)`
12. `docs: add README with config reference`
