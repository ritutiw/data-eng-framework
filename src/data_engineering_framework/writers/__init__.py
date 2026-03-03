"""Output writers for Delta Lake and other targets."""

from data_engineering_framework.writers.base import BaseWriter
from data_engineering_framework.writers.delta_writer import DeltaWriter

__all__ = ["BaseWriter", "DeltaWriter"]
