# Build pipeflow — Python ETL Pipeline

You are building a **portfolio project** for a Senior AI Engineer's public GitHub. It must be impressive, clean, and production-grade. Read these docs before writing any code:

1. **`P03-python-etl-pipeline.md`** — Complete project spec: architecture, phases, extractor/transform/loader pattern, commit plan. This is your primary blueprint. Follow it phase by phase.
2. **`github-portfolio.md`** — Portfolio goals and Definition of Done (Level 1 + Level 2). Understand the quality bar.
3. **`github-portfolio-checklist.md`** — Pre-publish checklist. Every item must pass before you're done.

---

## Instructions

### Read first, build second
Read all three docs completely before writing a single line of code. Understand the composable pipeline pattern, the extractor/transform/loader abstractions, Pydantic validation, and the CLI interface.

### Follow the phases in order
The project spec has 5 phases. Do them in order:
1. **Foundation** — project setup (pyproject.toml), core types, pipeline config schema, base classes
2. **Extractors** — CSV extractor, JSON extractor, HTTP/API extractor (async with httpx), file glob extractor
3. **Transforms** — composable transform steps: filter, map, rename, type cast, deduplicate, custom function. Chain them.
4. **Validation + Loading** — Pydantic schema validation, SQLite loader, PostgreSQL loader (asyncpg), CSV export loader
5. **CLI + Polish** — Click CLI interface (`pipeflow run config.yml`), comprehensive pytest suite, refactor, README

### Commit frequently
Follow the commit plan in the spec. Use **conventional commits**. Each commit should be a logical unit.

### Quality non-negotiables
- **Composable pipeline.** A pipeline is: extractor → [transforms] → validator → loader. Each piece is swappable. Defined in YAML config.
- **Base classes with clear contracts.** `BaseExtractor.extract() -> Iterator[dict]`, `BaseTransform.transform(record) -> dict`, `BaseLoader.load(records)`. New implementations just subclass.
- **Pydantic validation.** Records validated against a Pydantic model between transform and load. Invalid records captured with error details, not silently dropped.
- **Async where it matters.** HTTP extractor and PostgreSQL loader are async. CSV/file operations can be sync.
- **Error handling.** Bad records don't crash the pipeline. They're collected in an error report. Pipeline completes with summary: total, success, failed, skipped.
- **YAML configuration.** Pipelines defined in YAML files. The CLI reads config and executes. No hardcoded pipelines.
- **Comprehensive pytest.** Test each extractor, transform, and loader independently. Integration test for full pipeline. Use temp files and in-memory SQLite.
- **Type hints everywhere.** Full annotations. `mypy` should pass.
- **Lint clean.** Ruff for linting + formatting. Zero warnings.
- **No Docker.** Just `pip install` / `uv sync` and run.

### What NOT to do
- Don't use Airflow, Luigi, Prefect, or any ETL framework. This IS the framework.
- Don't use Pandas. Process records as dicts/Pydantic models. This shows you can build data pipelines without heavy dependencies.
- Don't skip the YAML config. Hardcoded pipelines defeat the purpose.
- Don't silently drop bad records. Capture them with error context.
- Don't commit `.venv/`, `__pycache__/`, or test database files.
- Don't leave `# TODO` or `# FIXME` comments anywhere.

---

## GitHub Username

The GitHub username is **devaloi**. For any GitHub URLs, use `github.com/devaloi/pipeflow`.

## Start

Read the three docs. Then begin Phase 1 from `P03-python-etl-pipeline.md`.
