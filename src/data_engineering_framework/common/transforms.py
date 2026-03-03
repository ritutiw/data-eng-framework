"""Message transformation utilities."""

from __future__ import annotations

from datetime import datetime, timezone


def flatten_dict(d: dict, parent_key: str = "", sep: str = "_") -> dict:
    items: list[tuple[str, object]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def add_ingestion_metadata(record: dict) -> dict:
    record["_ingested_at"] = datetime.now(tz=timezone.utc).isoformat()
    return record


def select_fields(record: dict, fields: list[str]) -> dict:
    return {k: record.get(k) for k in fields}


def rename_fields(record: dict, mapping: dict[str, str]) -> dict:
    return {mapping.get(k, k): v for k, v in record.items()}
