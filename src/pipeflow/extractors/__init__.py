"""Extractors package."""

from __future__ import annotations

from pipeflow.config import ExtractConfig
from pipeflow.extractors.base import Extractor


def build_extractor(config: ExtractConfig) -> Extractor:
    """Factory: build an extractor from config."""
    from pipeflow.extractors.csv_ext import CSVExtractor
    from pipeflow.extractors.json_ext import JSONExtractor
    from pipeflow.extractors.api import APIExtractor

    match config.type:
        case "csv":
            return CSVExtractor(
                path=config.path or "",
                delimiter=config.delimiter,
                encoding=config.encoding,
            )
        case "json" | "jsonl":
            return JSONExtractor(path=config.path or "", format=config.type)
        case "api":
            return APIExtractor(
                url=config.url or "",
                headers=config.headers,
                params=config.params,
                pagination=config.pagination,
            )
        case _:
            raise ValueError(f"Unknown extractor type: {config.type}")