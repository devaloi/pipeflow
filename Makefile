.PHONY: test lint typecheck run install dev

install:
	pip install -e .

dev:
	pip install -e ".[dev]"

test:
	python -m pytest -v

lint:
	python -m ruff check src/ tests/

typecheck:
	python -m mypy src/pipeflow/

run:
	python -m pipeflow run examples/csv_to_sqlite/pipeline.yaml
