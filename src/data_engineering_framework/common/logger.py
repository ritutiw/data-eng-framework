from __future__ import annotations

import logging
import os
from typing import Optional


def _setup_application_insights(connection_string: str) -> Optional[logging.Handler]:
    try:
        from opencensus.ext.azure.log_exporter import AzureLogHandler

        handler = AzureLogHandler(connection_string=connection_string)
        return handler
    except ImportError:
        logging.getLogger(__name__).warning(
            "opencensus-ext-azure not installed. Install with: pip install opencensus-ext-azure"
        )
        return None


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(console_handler)

    connection_string = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if connection_string:
        ai_handler = _setup_application_insights(connection_string)
        if ai_handler:
            ai_handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
            logger.addHandler(ai_handler)

    return logger
