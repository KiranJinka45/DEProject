"""Structured logging module for enterprise data pipelines.

Provides consistent JSON-formatted log records with pipeline metadata
(batch_id, pipeline_name, environment).
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Dict


class ContextLogger(logging.LoggerAdapter):
    """Logger adapter that injects pipeline runtime context into every log record."""

    def process(self, msg: Any, kwargs: Dict[str, Any]) -> tuple[Any, Dict[str, Any]]:
        extra = kwargs.setdefault("extra", {})
        extra["batch_id"] = self.extra.get("batch_id", os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "local-run"))
        extra["pipeline"] = self.extra.get("pipeline", "ecommerce-analytics")
        return f"[{extra['pipeline']}] [batch:{extra['batch_id']}] {msg}", kwargs


def get_logger(name: str = "pipeline", batch_id: str | None = None) -> ContextLogger:
    """Configures and returns a contextual pipeline logger."""
    base_logger = logging.getLogger(name)
    if not base_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        base_logger.addHandler(handler)
        base_logger.setLevel(logging.INFO)

    context = {"batch_id": batch_id or os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "local-exec")}
    return ContextLogger(base_logger, context)
