"""Tests for Pydantic record validation."""

from __future__ import annotations

from pipeflow.config import ValidateConfig
from pipeflow.validation.validator import build_validator


class TestRecordValidator:
    def test_valid_record_passes(self) -> None:
        config = ValidateConfig(
            model="User",
            fields={
                "name": {"type": "str"},
                "age": {"type": "int"},
            },
        )
        validator = build_validator(config)
        errors = validator.validate_record({"name": "Alice", "age": 30})
        assert errors == []

    def test_invalid_type_fails(self) -> None:
        config = ValidateConfig(
            model="User",
            fields={
                "name": {"type": "str"},
                "age": {"type": "int"},
            },
        )
        validator = build_validator(config)
        errors = validator.validate_record({"name": "Alice", "age": "not_a_number"})
        assert len(errors) > 0
        assert any(e["field"] == "age" for e in errors)

    def test_missing_required_field(self) -> None:
        config = ValidateConfig(
            model="User",
            fields={
                "name": {"type": "str", "required": True},
                "email": {"type": "str", "required": True},
            },
        )
        validator = build_validator(config)
        errors = validator.validate_record({"name": "Alice"})
        assert len(errors) > 0
        assert any(e["field"] == "email" for e in errors)

    def test_optional_field_missing_ok(self) -> None:
        config = ValidateConfig(
            model="User",
            fields={
                "name": {"type": "str", "required": True},
                "bio": {"type": "str", "required": False},
            },
        )
        validator = build_validator(config)
        errors = validator.validate_record({"name": "Alice"})
        assert errors == []

    def test_no_fields_permissive(self) -> None:
        config = ValidateConfig(model="AnyRecord")
        validator = build_validator(config)
        errors = validator.validate_record({"anything": "goes", "num": 42})
        assert errors == []

    def test_multiple_errors(self) -> None:
        config = ValidateConfig(
            model="User",
            fields={
                "name": {"type": "str"},
                "age": {"type": "int"},
                "score": {"type": "float"},
            },
        )
        validator = build_validator(config)
        errors = validator.validate_record(
            {"name": "Alice", "age": "bad", "score": "bad"}
        )
        assert len(errors) >= 2

    def test_error_structure(self) -> None:
        config = ValidateConfig(
            model="User",
            fields={"age": {"type": "int"}},
        )
        validator = build_validator(config)
        errors = validator.validate_record({"age": "not_int"})
        assert len(errors) == 1
        err = errors[0]
        assert "field" in err
        assert "message" in err
        assert "type" in err
