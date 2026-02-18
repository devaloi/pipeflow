"""Shared type mappings used across transforms and validation."""

from __future__ import annotations

from datetime import datetime
from typing import Any

TYPE_MAP: dict[str, type | Any] = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "datetime": datetime,
}

CAST_MAP: dict[str, type | Any] = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "datetime": datetime.fromisoformat,
}
