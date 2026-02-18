"""Loaders package."""

from __future__ import annotations

from pipeflow.config import LoadConfig


def build_loader(config: LoadConfig):  # type: ignore[return]
    """Factory: build a loader from config."""
    from pipeflow.loaders.sqlite import SQLiteLoader
    from pipeflow.loaders.csv_writer import CSVWriterLoader

    match config.type:
        case "sqlite":
            return SQLiteLoader(
                database=config.database or ":memory:",
                table=config.table or "data",
                mode=config.mode,
                conflict_key=config.conflict_key,
                batch_size=config.batch_size,
            )
        case "csv":
            return CSVWriterLoader(path=config.path or "output.csv")
        case _:
            raise ValueError(f"Unknown loader type: {config.type}")