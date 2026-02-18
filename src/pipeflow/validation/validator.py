"""Pydantic-based record validation."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ValidationError, create_model

from pipeflow.config import ValidateConfig
from pipeflow.lib.types import TYPE_MAP
from pipeflow.types import Record


class RecordValidator:
    """Validates records against a dynamically-built Pydantic model."""

    def __init__(self, model: type[BaseModel]) -> None:
        self.model = model

    def validate_record(self, record: Record) -> list[dict[str, Any]]:
        """Validate a single record. Returns list of errors (empty if valid)."""
        try:
            self.model.model_validate(record)
            return []
        except ValidationError as e:
            return [
                {
                    "field": ".".join(str(loc) for loc in err["loc"]),
                    "message": err["msg"],
                    "type": err["type"],
                }
                for err in e.errors()
            ]


def build_validator(config: ValidateConfig) -> RecordValidator:
    """Build a RecordValidator from config.

    If config.fields is provided, dynamically creates a Pydantic model.
    """
    if config.fields:
        field_definitions: dict[str, Any] = {}
        for field_name, field_spec in config.fields.items():
            field_type_str = field_spec.get("type", "str")
            field_type = TYPE_MAP.get(field_type_str, str)
            required = field_spec.get("required", True)
            if required:
                field_definitions[field_name] = (field_type, ...)
            else:
                field_definitions[field_name] = (field_type | None, None)

        model = create_model(config.model, **field_definitions)
        return RecordValidator(model=model)

    # Fallback: create a permissive model that accepts any fields
    model = create_model(config.model, __base__=BaseModel)
    return RecordValidator(model=model)
