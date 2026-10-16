"""Audit column and lineage tracking utilities.

Appends standardized audit metadata columns to dataframes across ingestion
and transformation stages for traceability and data governance.
"""

from __future__ import annotations

import datetime
import hashlib
from typing import Any
import pandas as pd


def compute_row_hash(row_values: list[Any]) -> str:
    """Computes a deterministic SHA256 hash for row-level change detection."""
    concatenated = "|".join("" if v is None else str(v) for v in row_values)
    return hashlib.sha256(concatenated.encode("utf-8")).hexdigest()


def enrich_with_audit_columns(
    df: pd.DataFrame,
    batch_id: str,
    source_identifier: str,
    id_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Enriches a pandas DataFrame with enterprise audit columns:

    - _load_ts: UTC timestamp of ingestion
    - _batch_id: Orchestration run or job execution identifier
    - _source_file: Original source file or upstream endpoint
    - _record_hash: SHA256 checksum of payload attributes for deduplication
    """
    df_enriched = df.copy()
    current_utc = datetime.datetime.now(datetime.timezone.utc)

    df_enriched["_load_ts"] = current_utc
    df_enriched["_batch_id"] = str(batch_id)
    df_enriched["_source_file"] = str(source_identifier)

    # Compute row hash across all data payload columns (excluding audit columns)
    cols_for_hash = id_columns if id_columns else [col for col in df.columns if not col.startswith("_")]
    
    df_enriched["_record_hash"] = df_enriched[cols_for_hash].astype(str).agg(
        lambda row: hashlib.sha256("|".join(row.values).encode("utf-8")).hexdigest(),
        axis=1,
    )

    return df_enriched
