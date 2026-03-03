from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from data_engineering_framework.common.logger import get_logger

logger = get_logger(__name__)


def deserialize_json(msg) -> Optional[dict]:
    try:
        value = json.loads(msg.value().decode("utf-8"))
        value["_kafka_topic"] = msg.topic()
        value["_kafka_partition"] = msg.partition()
        value["_kafka_offset"] = msg.offset()
        ts_type, ts_value = msg.timestamp()
        if ts_value and ts_value > 0:
            value["_kafka_timestamp"] = datetime.fromtimestamp(
                ts_value / 1000, tz=timezone.utc
            ).isoformat()
        else:
            value["_kafka_timestamp"] = datetime.now(tz=timezone.utc).isoformat()
        return value
    except Exception:
        logger.exception(
            "Failed to deserialize message at topic=%s partition=%d offset=%d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )
        return None
