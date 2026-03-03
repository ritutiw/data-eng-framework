import pyarrow as pa

from data_engineering_framework.processing.quality import (
    QualityValidator,
    RuleSeverity,
)


class TestQualityValidator:
    def test_not_null_passes(self):
        validator = QualityValidator()
        validator.add_not_null("name")

        table = pa.table({"name": ["alice", "bob"], "age": [30, 25]})
        results = validator.validate(table)

        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].failed_rows == 0

    def test_not_null_fails(self):
        validator = QualityValidator()
        validator.add_not_null("name")

        table = pa.table({"name": ["alice", None], "age": [30, 25]})
        results = validator.validate(table)

        assert len(results) == 1
        assert results[0].passed is False
        assert results[0].failed_rows == 1

    def test_unique_passes(self):
        validator = QualityValidator()
        validator.add_unique("id")

        table = pa.table({"id": [1, 2, 3]})
        results = validator.validate(table)

        assert results[0].passed is True

    def test_unique_fails(self):
        validator = QualityValidator()
        validator.add_unique("id")

        table = pa.table({"id": [1, 2, 2]})
        results = validator.validate(table)

        assert results[0].passed is False
        assert results[0].failed_rows == 1

    def test_has_errors(self):
        validator = QualityValidator()
        validator.add_not_null("name", severity=RuleSeverity.ERROR)

        table = pa.table({"name": ["alice", None]})
        results = validator.validate(table)

        assert validator.has_errors(results) is True

    def test_warning_not_error(self):
        validator = QualityValidator()
        validator.add_not_null("name", severity=RuleSeverity.WARNING)

        table = pa.table({"name": ["alice", None]})
        results = validator.validate(table)

        assert validator.has_errors(results) is False

    def test_missing_column_skipped(self):
        validator = QualityValidator()
        validator.add_not_null("nonexistent")

        table = pa.table({"name": ["alice"]})
        results = validator.validate(table)

        assert len(results) == 0
