"""Pipeline configuration models parsed from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ExtractConfig(BaseModel):
    """Configuration for the extraction step."""

    type: str
    path: str | None = None
    url: str | None = None
    delimiter: str = ","
    encoding: str = "utf-8"
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, str] = Field(default_factory=dict)
    pagination: dict[str, Any] | None = None


class TransformConfig(BaseModel):
    """Configuration for a single transform step."""

    type: str
    mapping: dict[str, str] | None = None
    columns: dict[str, str] | None = None
    condition: str | None = None
    expression: str | None = None
    key: str | list[str] | None = None


class ValidateConfig(BaseModel):
    """Configuration for the validation step."""

    model: str
    fields: dict[str, dict[str, Any]] | None = None


class LoadConfig(BaseModel):
    """Configuration for the load step."""

    type: str
    database: str | None = None
    table: str | None = None
    path: str | None = None
    mode: str = "insert"
    conflict_key: str | None = None
    batch_size: int = 100


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration."""

    name: str
    extract: ExtractConfig
    transforms: list[TransformConfig] = Field(default_factory=list)
    validate: ValidateConfig | None = Field(default=None, alias="validate")
    load: LoadConfig

    model_config = {"populate_by_name": True}


def load_config(path: str | Path) -> PipelineConfig:
    """Load and validate a pipeline config from a YAML file."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Invalid config: expected a YAML mapping, got {type(raw).__name__}")

    return PipelineConfig(**raw)
