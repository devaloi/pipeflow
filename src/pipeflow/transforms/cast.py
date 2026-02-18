"""Type casting transform."""

from __future__ import annotations

from datetime import datetime
from typing import Any


Record = dict[str, Any]

CAST_MAP: dict[str, type | Any] = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "datetime": datetime.fromisoformat,
}


class CastTransform:
    """Cast column values to specified types."""

    def __init__(self, columns: dict[str, str]) -> None:
        self.columns = columns  # column_name -> type_name

    def apply(self, record: Record) -> Record:
        result = dict(record)
        for col, type_name in self.columns.items():
            if col in result and result[col] is not None:
                caster = CAST_MAP.get(type_name)
                if caster is None:
                    raise ValueError(f"Unknown cast type: {type_name}")
                try:
                    result[col] = caster(result[col])
                except (ValueError, TypeError) as e:
                    raise ValueError(
                        f"Cannot cast column '{col}' value {result[col]!r} to {type_name}: {e}"
                    ) from e
        return result
