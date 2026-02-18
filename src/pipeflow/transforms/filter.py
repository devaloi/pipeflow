"""Filter rows by predicate expression."""

from __future__ import annotations

from pipeflow.lib.safe_eval import safe_eval
from pipeflow.types import Record


class FilterTransform:
    """Filter records using a boolean expression.

    Uses a restricted AST-based evaluator with record values in scope.
    Returns None for records that don't match the condition.
    """

    def __init__(self, condition: str) -> None:
        self.condition = condition

    def apply(self, record: Record) -> Record | None:
        try:
            result = safe_eval(self.condition, record)
        except Exception as e:
            raise ValueError(
                f"Failed to evaluate filter condition {self.condition!r}: {e}"
            ) from e
        return record if result else None
