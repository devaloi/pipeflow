"""Derive new columns from expressions."""

from __future__ import annotations

from pipeflow.lib.safe_eval import safe_eval
from pipeflow.types import Record


class DeriveTransform:
    """Compute a new column from an expression.

    Expression format: "new_col = expr" where expr uses record field names.
    Uses a restricted AST-based evaluator with only record values in scope.
    """

    def __init__(self, expression: str) -> None:
        if "=" not in expression:
            raise ValueError(f"Derive expression must contain '=': {expression!r}")
        parts = expression.split("=", 1)
        self.target = parts[0].strip()
        self.expr = parts[1].strip()

    def apply(self, record: Record) -> Record:
        result = dict(record)
        try:
            result[self.target] = safe_eval(self.expr, result)
        except Exception as e:
            raise ValueError(f"Failed to evaluate derive expression {self.expr!r}: {e}") from e
        return result
