# Contributing to pipeflow

Thank you for your interest in contributing!

## Development Setup

```bash
git clone https://github.com/devaloi/pipeflow.git
cd pipeflow
python -m venv .venv && source .venv/bin/activate\npip install -e ".[dev]"
```

### Prerequisites

- Python 3.11+

## Running Tests

```bash
make test
make lint
make all
```

## Pull Request Guidelines

- One feature or fix per PR
- Run `make all` before submitting
- Add tests for new functionality
- Update README if adding a new feature

## Reporting Issues

Open a GitHub issue with your language/runtime version, steps to reproduce, and expected vs actual behavior.
