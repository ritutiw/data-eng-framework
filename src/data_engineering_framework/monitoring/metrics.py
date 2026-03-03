"""Metrics collection and reporting."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from data_engineering_framework.common.logger import get_logger

logger = get_logger(__name__)


@dataclass
class PipelineMetrics:
    pipeline_name: str
    start_time: float = field(default_factory=time.monotonic)
    end_time: float = 0.0
    rows_ingested: int = 0
    rows_processed: int = 0
    rows_written: int = 0
    errors: int = 0

    @property
    def duration_seconds(self) -> float:
        end = self.end_time or time.monotonic()
        return end - self.start_time

    @property
    def throughput(self) -> float:
        duration = self.duration_seconds
        if duration == 0:
            return 0.0
        return self.rows_written / duration

    def complete(self) -> None:
        self.end_time = time.monotonic()
        logger.info(
            "Pipeline '%s' metrics: ingested=%d processed=%d written=%d "
            "errors=%d duration=%.2fs throughput=%.1f rows/s",
            self.pipeline_name,
            self.rows_ingested,
            self.rows_processed,
            self.rows_written,
            self.errors,
            self.duration_seconds,
            self.throughput,
        )


class MetricsCollector:
    def __init__(self):
        self._metrics: dict[str, PipelineMetrics] = {}

    def start(self, pipeline_name: str) -> PipelineMetrics:
        metrics = PipelineMetrics(pipeline_name=pipeline_name)
        self._metrics[pipeline_name] = metrics
        return metrics

    def get(self, pipeline_name: str) -> PipelineMetrics | None:
        return self._metrics.get(pipeline_name)

    def all_metrics(self) -> dict[str, PipelineMetrics]:
        return dict(self._metrics)
