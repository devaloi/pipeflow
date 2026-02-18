"""Filter rows by predicate expression."""

from __future__ import annotations

from typing import Any


Record = dict[str, Any]


class FilterTransform:
    """Filter records using a boolean expression.

    Uses restricted eval with record values in scope.
    Returns None for records that don't match the condition.
    """

    def __init__(self, condition: str) -> None:
        self.condition = condition

    def apply(self, record: Record) -> Record | None:
        safe_globals: dict[str, Any] = {"__builtins__": {
            "len": len, "str": str, "int": int, "float": float,
            "abs": abs, "min": min, "max": max, "True": True, "False": False,
            "None": None,
        }}
        try:
            result = eval(self.condition, safe_globals, record)  # noqa: S307
        except Exception as e:
            raise ValueError(
                f"Failed to evaluate filter condition {self.condition!r}: {e}"
            ) from e
        return record if result else None
