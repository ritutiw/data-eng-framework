"""Data quality validation framework for Silver layer."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

import pyarrow as pa
import pyarrow.compute as pc

from data_engineering_framework.common.logger import get_logger

logger = get_logger(__name__)


class RuleSeverity(Enum):
    WARNING = "warning"
    ERROR = "error"


@dataclass
class QualityRule:
    name: str
    column: str
    check: Callable[[pa.Array], pa.BooleanArray]
    severity: RuleSeverity = RuleSeverity.ERROR


@dataclass
class QualityResult:
    rule_name: str
    column: str
    total_rows: int
    failed_rows: int
    severity: RuleSeverity
    passed: bool = field(init=False)

    def __post_init__(self):
        self.passed = self.failed_rows == 0


class QualityValidator:
    def __init__(self, rules: list[QualityRule] | None = None):
        self._rules: list[QualityRule] = rules or []

    def add_not_null(self, column: str, severity: RuleSeverity = RuleSeverity.ERROR):
        self._rules.append(
            QualityRule(
                name=f"not_null_{column}",
                column=column,
                check=pc.is_valid,
                severity=severity,
            )
        )

    def add_unique(self, column: str, severity: RuleSeverity = RuleSeverity.ERROR):
        def check_unique(arr: pa.Array) -> pa.BooleanArray:
            seen: set = set()
            results = []
            for val in arr:
                py_val = val.as_py()
                results.append(py_val not in seen)
                seen.add(py_val)
            return pa.array(results, type=pa.bool_())

        self._rules.append(
            QualityRule(
                name=f"unique_{column}",
                column=column,
                check=check_unique,
                severity=severity,
            )
        )

    def validate(self, table: pa.Table) -> list[QualityResult]:
        results: list[QualityResult] = []
        for rule in self._rules:
            if rule.column not in table.column_names:
                logger.warning("Column '%s' not found, skipping rule '%s'", rule.column, rule.name)
                continue

            col = table.column(rule.column)
            mask = rule.check(col)
            failed = pc.sum(pc.invert(mask)).as_py()

            result = QualityResult(
                rule_name=rule.name,
                column=rule.column,
                total_rows=table.num_rows,
                failed_rows=failed,
                severity=rule.severity,
            )
            results.append(result)

            if not result.passed:
                logger.log(
                    logging.ERROR if rule.severity == RuleSeverity.ERROR else logging.WARNING,
                    "Quality check '%s' failed: %d/%d rows",
                    rule.name,
                    failed,
                    table.num_rows,
                )

        return results

    def has_errors(self, results: list[QualityResult]) -> bool:
        return any(not r.passed and r.severity == RuleSeverity.ERROR for r in results)
