"""Tests for all transform modules."""

from __future__ import annotations

from datetime import datetime

import pytest

from pipeflow.transforms.rename import RenameTransform
from pipeflow.transforms.cast import CastTransform
from pipeflow.transforms.derive import DeriveTransform
from pipeflow.transforms.filter import FilterTransform
from pipeflow.transforms.deduplicate import DeduplicateTransform


class TestRenameTransform:
    def test_basic_rename(self) -> None:
        t = RenameTransform(mapping={"Full Name": "name", "Email Address": "email"})
        record = {"Full Name": "Alice", "Email Address": "alice@example.com", "age": 30}
        result = t.apply(record)
        assert result == {"name": "Alice", "email": "alice@example.com", "age": 30}

    def test_no_match_keys_unchanged(self) -> None:
        t = RenameTransform(mapping={"foo": "bar"})
        record = {"name": "Alice"}
        assert t.apply(record) == {"name": "Alice"}

    def test_empty_mapping(self) -> None:
        t = RenameTransform(mapping={})
        record = {"a": 1, "b": 2}
        assert t.apply(record) == {"a": 1, "b": 2}


class TestCastTransform:
    def test_str_to_int(self) -> None:
        t = CastTransform(columns={"age": "int"})
        result = t.apply({"age": "25", "name": "Bob"})
        assert result["age"] == 25
        assert isinstance(result["age"], int)

    def test_str_to_float(self) -> None:
        t = CastTransform(columns={"score": "float"})
        result = t.apply({"score": "95.5"})
        assert result["score"] == 95.5

    def test_str_to_datetime(self) -> None:
        t = CastTransform(columns={"created": "datetime"})
        result = t.apply({"created": "2024-01-15"})
        assert isinstance(result["created"], datetime)
        assert result["created"].year == 2024

    def test_invalid_cast_raises(self) -> None:
        t = CastTransform(columns={"age": "int"})
        with pytest.raises(ValueError, match="Cannot cast"):
            t.apply({"age": "not_a_number"})

    def test_missing_column_skipped(self) -> None:
        t = CastTransform(columns={"missing": "int"})
        result = t.apply({"name": "Alice"})
        assert result == {"name": "Alice"}

    def test_none_value_skipped(self) -> None:
        t = CastTransform(columns={"age": "int"})
        result = t.apply({"age": None})
        assert result["age"] is None


class TestDeriveTransform:
    def test_simple_concatenation(self) -> None:
        t = DeriveTransform(expression="full_name = first + ' ' + last")
        result = t.apply({"first": "Alice", "last": "Smith"})
        assert result["full_name"] == "Alice Smith"

    def test_arithmetic(self) -> None:
        t = DeriveTransform(expression="total = price * quantity")
        result = t.apply({"price": 10, "quantity": 3})
        assert result["total"] == 30

    def test_original_fields_preserved(self) -> None:
        t = DeriveTransform(expression="upper_name = name.upper()")
        result = t.apply({"name": "alice"})
        assert result["name"] == "alice"
        assert result["upper_name"] == "ALICE"

    def test_invalid_expression(self) -> None:
        with pytest.raises(ValueError, match="must contain '='"):
            DeriveTransform(expression="no_equals_sign")


class TestFilterTransform:
    def test_matching_record_returned(self) -> None:
        t = FilterTransform(condition="age >= 18")
        result = t.apply({"name": "Alice", "age": 30})
        assert result is not None
        assert result["name"] == "Alice"

    def test_non_matching_record_filtered(self) -> None:
        t = FilterTransform(condition="age >= 18")
        result = t.apply({"name": "Diana", "age": 17})
        assert result is None

    def test_string_comparison(self) -> None:
        t = FilterTransform(condition="city != 'NYC'")
        assert t.apply({"city": "LA"}) is not None
        assert t.apply({"city": "NYC"}) is None

    def test_complex_condition(self) -> None:
        t = FilterTransform(condition="age >= 18 and name != 'Bob'")
        assert t.apply({"name": "Alice", "age": 30}) is not None
        assert t.apply({"name": "Bob", "age": 25}) is None


class TestDeduplicateTransform:
    def test_removes_duplicates(self) -> None:
        t = DeduplicateTransform(key=["email"])
        r1 = t.apply({"email": "alice@example.com", "name": "Alice"})
        r2 = t.apply({"email": "alice@example.com", "name": "Alice Again"})
        r3 = t.apply({"email": "bob@example.com", "name": "Bob"})
        assert r1 is not None
        assert r2 is None  # duplicate
        assert r3 is not None

    def test_composite_key(self) -> None:
        t = DeduplicateTransform(key=["first", "last"])
        r1 = t.apply({"first": "John", "last": "Doe"})
        r2 = t.apply({"first": "John", "last": "Smith"})
        r3 = t.apply({"first": "John", "last": "Doe"})
        assert r1 is not None
        assert r2 is not None
        assert r3 is None

    def test_reset(self) -> None:
        t = DeduplicateTransform(key=["id"])
        t.apply({"id": 1})
        t.reset()
        result = t.apply({"id": 1})
        assert result is not None


class TestTransformChain:
    """Test composing multiple transforms."""

    def test_rename_then_cast(self) -> None:
        rename = RenameTransform(mapping={"Age": "age"})
        cast = CastTransform(columns={"age": "int"})
        record = {"Age": "25", "name": "Bob"}
        result = rename.apply(record)
        result = cast.apply(result)
        assert result == {"age": 25, "name": "Bob"}

    def test_chain_with_filter(self) -> None:
        cast = CastTransform(columns={"age": "int"})
        filt = FilterTransform(condition="age >= 18")
        records = [
            {"name": "Alice", "age": "30"},
            {"name": "Diana", "age": "17"},
            {"name": "Bob", "age": "25"},
        ]
        results = []
        for rec in records:
            r = cast.apply(rec)
            r = filt.apply(r)
            if r is not None:
                results.append(r)
        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Bob"
