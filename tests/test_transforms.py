"""Tests for transform utilities."""

from kafka_delta_sink.transforms import (
    add_ingestion_metadata,
    flatten_dict,
    rename_fields,
    select_fields,
)


class TestFlattenDict:
    def test_flat_dict_unchanged(self):
        assert flatten_dict({"a": 1, "b": 2}) == {"a": 1, "b": 2}

    def test_nested_dict(self):
        result = flatten_dict({"a": {"b": 1, "c": {"d": 2}}})
        assert result == {"a_b": 1, "a_c_d": 2}

    def test_empty_dict(self):
        assert flatten_dict({}) == {}


class TestAddIngestionMetadata:
    def test_adds_ingested_at(self):
        record = {"key": "value"}
        result = add_ingestion_metadata(record)
        assert "_ingested_at" in result
        assert result["key"] == "value"


class TestSelectFields:
    def test_select_existing_fields(self):
        record = {"a": 1, "b": 2, "c": 3}
        result = select_fields(record, ["a", "c"])
        assert result == {"a": 1, "c": 3}

    def test_select_missing_field_returns_none(self):
        record = {"a": 1}
        result = select_fields(record, ["a", "missing"])
        assert result == {"a": 1, "missing": None}


class TestRenameFields:
    def test_rename(self):
        record = {"old_name": 1, "keep": 2}
        result = rename_fields(record, {"old_name": "new_name"})
        assert result == {"new_name": 1, "keep": 2}

    def test_no_mapping(self):
        record = {"a": 1}
        result = rename_fields(record, {})
        assert result == {"a": 1}
