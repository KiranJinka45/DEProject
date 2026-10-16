"""Unit tests for audit enrichment metadata and row hash computation."""

import pandas as pd
from src.utils.audit import enrich_with_audit_columns, compute_row_hash


def test_enrich_with_audit_columns():
    df_sample = pd.DataFrame([
        {"id": "A1", "name": "Alice", "country": "US"},
        {"id": "A2", "name": "Bob", "country": "CA"},
    ])

    batch_id = "test-batch-001"
    source = "sample.csv"

    df_enriched = enrich_with_audit_columns(df_sample, batch_id=batch_id, source_identifier=source)

    assert "_load_ts" in df_enriched.columns
    assert "_batch_id" in df_enriched.columns
    assert "_source_file" in df_enriched.columns
    assert "_record_hash" in df_enriched.columns

    assert df_enriched["_batch_id"].iloc[0] == batch_id
    assert df_enriched["_source_file"].iloc[0] == source
    assert len(df_enriched["_record_hash"].iloc[0]) == 64  # SHA256 hex length


def test_deterministic_row_hash():
    hash1 = compute_row_hash(["CUST1", "alice@example.com", "US"])
    hash2 = compute_row_hash(["CUST1", "alice@example.com", "US"])
    hash3 = compute_row_hash(["CUST1", "alice_new@example.com", "US"])

    assert hash1 == hash2
    assert hash1 != hash3
