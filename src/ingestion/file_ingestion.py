"""File ingestion engine with defensive validation and quarantine routing.

Reads raw CSV/JSON records, validates against Pydantic Data Contracts,
enriches valid rows with audit columns, and loads to Bronze storage/PostgreSQL.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Type
import pandas as pd
from pydantic import BaseModel, ValidationError
from sqlalchemy import create_engine

from src.ingestion.schemas import (
    CustomerContract,
    MarketingSpendContract,
    OrderContract,
    OrderItemContract,
    ProductContract,
)
from src.utils.audit import enrich_with_audit_columns
from src.utils.logger import get_logger

logger = get_logger("file-ingestion")

CONTRACT_REGISTRY: dict[str, Type[BaseModel]] = {
    "orders": OrderContract,
    "order_items": OrderItemContract,
    "customers": CustomerContract,
    "products": ProductContract,
    "marketing_spend": MarketingSpendContract,
}


class FileIngestionPipeline:
    """Ingestion pipeline handling file reading, schema validation, quarantine routing,

    and Bronze layer loading.
    """

    def __init__(self, db_conn_str: str | None = None):
        self.db_conn_str = db_conn_str or os.getenv(
            "WAREHOUSE_CONN_STR",
            "postgresql+psycopg2://dw_admin:dw_secure_pass@localhost:5432/ecommerce_warehouse",
        )
        self.engine = create_engine(self.db_conn_str)

    def ingest_dataset(
        self,
        dataset_name: str,
        input_file: Path | str,
        batch_id: str,
        quarantine_dir: Path | str = "data/quarantine",
        output_bronze_dir: Path | str = "data/bronze",
    ) -> dict[str, Any]:
        """Validates and processes input file into Bronze layer with audit columns."""
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        contract_cls = CONTRACT_REGISTRY.get(dataset_name)
        if not contract_cls:
            raise ValueError(f"Unknown dataset '{dataset_name}'. Available: {list(CONTRACT_REGISTRY.keys())}")

        logger.info(f"Starting ingestion for '{dataset_name}' from {input_path.name}")

        # 1. Read raw input data
        if input_path.suffix.lower() == ".json":
            df_raw = pd.read_json(input_path)
        elif input_path.suffix.lower() == ".csv":
            df_raw = pd.read_csv(input_path)
        elif input_path.suffix.lower() == ".parquet":
            df_raw = pd.read_parquet(input_path)
        else:
            raise ValueError(f"Unsupported file format: {input_path.suffix}")

        records = df_raw.to_dict(orient="records")
        valid_records: list[dict[str, Any]] = []
        quarantine_records: list[dict[str, Any]] = []

        # 2. Defensive schema validation per row
        for record in records:
            try:
                validated = contract_cls(**record)
                valid_records.append(validated.model_dump())
            except ValidationError as err:
                quarantine_records.append(
                    {
                        "raw_record": record,
                        "validation_errors": err.errors(),
                        "batch_id": batch_id,
                        "source_file": input_path.name,
                    }
                )

        # 3. Route invalid records to quarantine directory
        if quarantine_records:
            q_dir = Path(quarantine_dir) / dataset_name
            q_dir.mkdir(parents=True, exist_ok=True)
            quarantine_file = q_dir / f"quarantine_{batch_id}_{input_path.stem}.json"
            with open(quarantine_file, "w", encoding="utf-8") as f:
                json.dump(quarantine_records, f, default=str, indent=2)
            logger.warning(
                f"Quarantined {len(quarantine_records)} invalid records to {quarantine_file}"
            )

        # 4. Enrich valid records with audit columns
        if valid_records:
            df_valid = pd.DataFrame(valid_records)
            df_bronze = enrich_with_audit_columns(
                df=df_valid,
                batch_id=batch_id,
                source_identifier=input_path.name,
            )

            # 5. Persist to Bronze Local Lakehouse (Parquet)
            b_dir = Path(output_bronze_dir) / dataset_name
            b_dir.mkdir(parents=True, exist_ok=True)
            bronze_parquet = b_dir / f"bronze_{dataset_name}_{batch_id}.parquet"
            df_bronze.to_parquet(bronze_parquet, index=False)
            logger.info(f"Saved {len(df_bronze)} records to Bronze lakehouse: {bronze_parquet}")

            # 6. Load into PostgreSQL bronze schema
            try:
                table_name = f"bronze_{dataset_name}"
                df_bronze.to_sql(
                    name=table_name,
                    con=self.engine,
                    schema="bronze",
                    if_exists="append",
                    index=False,
                )
                logger.info(f"Loaded {len(df_bronze)} records into table bronze.{table_name}")
            except Exception as e:
                logger.error(f"Failed loading into PostgreSQL bronze schema: {e}")
                # We do not fail the task if local parquet persisted, but re-raise for visibility if needed
                raise

        summary = {
            "dataset": dataset_name,
            "total_records": len(records),
            "valid_records": len(valid_records),
            "quarantined_records": len(quarantine_records),
            "batch_id": batch_id,
        }
        return summary
