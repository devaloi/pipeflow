"""Derive new columns from expressions."""

from __future__ import annotations

from typing import Any


Record = dict[str, Any]


class DeriveTransform:
    """Compute a new column from an expression.

    Expression format: "new_col = expr" where expr uses record field names.
    Uses a restricted eval with only record values in scope.
    """

    def __init__(self, expression: str) -> None:
        if "=" not in expression:
            raise ValueError(f"Derive expression must contain '=': {expression!r}")
        parts = expression.split("=", 1)
        self.target = parts[0].strip()
        self.expr = parts[1].strip()

    def apply(self, record: Record) -> Record:
        result = dict(record)
        # Restricted scope: only record keys + basic builtins
        safe_globals: dict[str, Any] = {"__builtins__": {
            "len": len, "str": str, "int": int, "float": float,
            "round": round, "abs": abs, "min": min, "max": max,
            "upper": str.upper, "lower": str.lower,
        }}
        try:
            result[self.target] = eval(self.expr, safe_globals, result)  # noqa: S307
        except Exception as e:
            raise ValueError(f"Failed to evaluate derive expression {self.expr!r}: {e}") from e
        return result
