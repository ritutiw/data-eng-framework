"""Tests for DeltaWriter using local filesystem (no ADLS needed)."""

import pyarrow as pa
from deltalake import DeltaTable

from kafka_delta_sink.delta_writer import DeltaWriter


class TestDeltaWriter:
    def test_write_creates_table(self, sample_schema, sample_records, tmp_delta_path):
        writer = DeltaWriter(
            table_uri=tmp_delta_path,
            storage_options={},
        )
        table = pa.Table.from_pylist(sample_records, schema=sample_schema)
        writer.write(table)

        assert writer.table_exists()
        dt = DeltaTable(tmp_delta_path)
        result = dt.to_pyarrow_table()
        assert result.num_rows == 2

    def test_write_append(self, sample_schema, sample_records, tmp_delta_path):
        writer = DeltaWriter(
            table_uri=tmp_delta_path,
            storage_options={},
        )
        table = pa.Table.from_pylist(sample_records, schema=sample_schema)

        writer.write(table)
        writer.write(table)

        dt = DeltaTable(tmp_delta_path)
        result = dt.to_pyarrow_table()
        assert result.num_rows == 4

    def test_write_with_partition(self, sample_schema, sample_records, tmp_delta_path):
        writer = DeltaWriter(
            table_uri=tmp_delta_path,
            storage_options={},
            partition_by=["event_type"],
        )
        table = pa.Table.from_pylist(sample_records, schema=sample_schema)
        writer.write(table)

        dt = DeltaTable(tmp_delta_path)
        result = dt.to_pyarrow_table()
        assert result.num_rows == 2

    def test_get_version(self, sample_schema, sample_records, tmp_delta_path):
        writer = DeltaWriter(
            table_uri=tmp_delta_path,
            storage_options={},
        )
        table = pa.Table.from_pylist(sample_records, schema=sample_schema)

        writer.write(table)
        assert writer.get_version() == 0

        writer.write(table)
        assert writer.get_version() == 1

    def test_table_exists_false(self, tmp_delta_path):
        writer = DeltaWriter(
            table_uri=tmp_delta_path,
            storage_options={},
        )
        assert not writer.table_exists()
