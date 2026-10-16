# Technical Design Document & Product Requirement Document (PRD)
## Enterprise E-Commerce Analytics Platform (Medallion Architecture)

- **Document Version:** 1.0.0
- **Status:** APPROVED / PRODUCTION READY
- **Author:** Staff Data Architect & Principal Data Engineer
- **Target Audience:** Data Platform Engineers, Analytics Engineers, Business Intelligence Leaders

---

## 1. Executive Summary & Business Objectives

Modern omnichannel e-commerce requires immediate, unified visibility into customer behavior, inventory economics, multi-touch advertising efficiency, and revenue recognition. Fragmented data across OLTP transactional databases, third-party advertising APIs (Meta Ads, Google Ads, TikTok), logistics endpoints, and front-end clickstream telemetry hinders accurate calculations of:
- **CAC (Customer Acquisition Cost)** and **ROAS (Return on Ad Spend)**
- **LTV (Customer Lifetime Value)** across acquisition cohorts
- **AOV (Average Order Value)** and gross profit margin by product line
- **Retention & Churn Dynamics** via point-in-time historical tracking
- **First-party Click-to-Purchase Attribution**

This platform establishes a decoupled, idempotent, and resilient **Medallion Lakehouse Architecture** leveraging Apache Airflow, PySpark/Python, PostgreSQL, dbt, and Great Expectations/Soda.

---

## 2. Architectural Specification

```
+---------------------------------------------------------------------------------------------------------+
|                                           DATA PRODUCERS                                                |
|  [OLTP Database: Orders & Items]   [Ad APIs: Meta / Google]   [Catalog System]   [Clickstream Telemetry]|
+---------------------------------------------------------------------------------------------------------+
                                                     |
                                                     v (Defensive Ingestion / Contracts / Dead-Letter-Queue)
+---------------------------------------------------------------------------------------------------------+
|                                        BRONZE LAYER (RAW / IMMUTABLE)                                   |
|  - Storage: MinIO Lakehouse (S3 Parquet) + PostgreSQL Staging (bronze schema)                          |
|  - Ingestion: Partitioned by year=YYYY/month=MM/day=DD                                                  |
|  - Audit Columns: _load_ts, _batch_id, _source_file, _record_hash                                       |
|  - DLQ Quarantine: data/quarantine/{dataset} for schema contract violators                             |
+---------------------------------------------------------------------------------------------------------+
                                                     |
                                                     v (dbt Staging Models, Typecasting, Deduplication, Snapshots)
+---------------------------------------------------------------------------------------------------------+
|                                      SILVER LAYER (STANDARDIZED / ATOMIC)                               |
|  - Data Cleansing: Deduplication with window functions (row_number() over partition by PK)             |
|  - Normalization: Multi-currency conversion to standardized USD (case-when logic / FX feeds)           |
|  - Slowly Changing Dimensions: dbt snapshots tracking customer attribute history (SCD Type 2)           |
+---------------------------------------------------------------------------------------------------------+
                                                     |
                                                     v (dbt Dimensional Marts, Star Schema, Referential Integrity)
+---------------------------------------------------------------------------------------------------------+
|                                     GOLD LAYER (CURATED STAR SCHEMA / MARTS)                            |
|  - fact_orders: Incremental order grain, joins SCD Type 2 customer records point-in-time                 |
|  - fact_order_items: Line-item grain with calculated net revenue, cost of goods, gross margin           |
|  - dim_customer: Historical SCD2 customer profiles with lifetime orders, revenue, LTV tiers             |
|  - dim_product: Product catalog with SKU, taxonomy, margin classifications                             |
|  - dim_marketing_channel: Aggregated ad spend, impressions, CTR, CPC, CPA                               |
|  - dim_date: Conformed calendar dimension from 2022 to 2030                                             |
+---------------------------------------------------------------------------------------------------------+
                                                     |
                                                     v
+---------------------------------------------------------------------------------------------------------+
|                                            BI & SERVING LAYER                                           |
|  [Power BI / Metabase Dashboards] <---- Direct SQL Queries / Cached Semantic Models                     |
+---------------------------------------------------------------------------------------------------------+
```

### Storage Formats, Partitioning & Compression Strategies
- **Bronze (Raw Lakehouse):** Apache Parquet format compressed with **Snappy**. Partitioned hierarchically by `/year=YYYY/month=MM/day=DD/` to maximize predicate pushdown and pruning during downstream reads.
- **Relational Bronze & Silver:** PostgreSQL schemas (`bronze`, `silver`) optimized with B-Tree indexes on primary identifiers (`order_id`, `customer_id`, `updated_at`, `_load_ts`).
- **Gold Marts:** Normalized star schema hosted in PostgreSQL `gold` schema (or DuckDB/Snowflake), utilizing columnar indices where applicable, with surrogate key references (`*_sk`).

---

## 3. Data Contracts & Schema Specifications

### Ingestion Contracts (Pydantic V2 Enforced)
1. **Orders (`OrderContract`):**
   - `order_id` (string, required)
   - `customer_id` (string, required)
   - `order_status` (string, enum: pending, processing, completed, shipped, delivered, cancelled, refunded)
   - `order_timestamp` (datetime ISO 8601 UTC)
   - `total_amount` (decimal, >= 0)
   - `currency` (string, 3-char ISO code)
   - `shipping_address` (string)
   - `payment_method` (string)

2. **Order Items (`OrderItemContract`):**
   - `order_item_id` (string, primary key)
   - `order_id` (string, foreign key)
   - `product_id` (string, foreign key)
   - `quantity` (integer, > 0)
   - `unit_price` (decimal, >= 0)
   - `discount_amount` (decimal, >= 0)
   - `subtotal` (decimal, >= 0)

3. **Customer Profiles (`CustomerContract`):**
   - `customer_id` (string, primary key)
   - `email` (string, validated email format)
   - `first_name`, `last_name` (string)
   - `country_code` (string, 2-letter ISO)
   - `city` (string)
   - `signup_timestamp` (datetime UTC)
   - `updated_at` (datetime UTC)

4. **Marketing Ad Spend (`MarketingSpendContract`):**
   - `campaign_id` (string, primary key)
   - `channel` (string: Google Ads, Meta Ads, TikTok Ads)
   - `spend_date` (date)
   - `spend_amount` (decimal, >= 0)
   - `impressions` (int), `clicks` (int), `conversions` (int)

---

## 4. Dimensional Modeling (Gold Layer Star Schema)

```
                            +-----------------------+
                            |       dim_date        |
                            +-----------------------+
                            | date_sk (PK)          |
                            | calendar_date         |
                            | year_number           |
                            | quarter_number        |
                            | month_number          |
                            | is_weekend            |
                            +-----------------------+
                                        |
                 +----------------------+----------------------+
                 |                                             |
                 v                                             v
+-------------------------------+             +-------------------------------+
|          fact_orders          |             |       fact_order_items        |
+-------------------------------+             +-------------------------------+
| order_sk (PK)                 | 1         N | order_item_sk (PK)            |
| order_id                      |-------------| order_sk (FK)                 |
| order_date_sk (FK)            |             | order_date_sk (FK)            |
| customer_sk (FK)              |             | product_sk (FK)               |
| customer_id                   |             | quantity                      |
| total_amount_usd              |             | unit_price                    |
| total_line_items              |             | net_subtotal_usd              |
| customer_order_sequence       |             | total_cost_usd                |
| is_first_order                |             | gross_margin_usd              |
+-------------------------------+             +-------------------------------+
                 |                                             |
                 v                                             v
+-------------------------------+             +-------------------------------+
|      dim_customer (SCD2)      |             |          dim_product          |
+-------------------------------+             +-------------------------------+
| customer_sk (PK)              |             | product_sk (PK)               |
| customer_id                   |             | product_id                    |
| email                         |             | sku                           |
| full_name                     |             | product_name                  |
| city, country_code            |             | category                      |
| valid_from                    |             | cost_price, retail_price      |
| valid_to                      |             | margin_percentage             |
| is_current_record             |             | margin_tier                   |
| total_lifetime_orders         |             +-------------------------------+
| lifetime_revenue_usd          |
| customer_ltv_tier             |
+-------------------------------+
```

### SCD Type 2 Implementation
Changes to customer profiles (e.g. shipping address, email, residence) are tracked with non-destructive versioning:
- `valid_from`: Timestamp when the customer record became active.
- `valid_to`: Timestamp when superseded (defaults to `'9999-12-31 23:59:59+00'`).
- `is_current_record`: Boolean flag indicating current active state.
- **Fact Joins:** `fact_orders` joins to `dim_customer` on `order_timestamp >= valid_from AND order_timestamp < valid_to` to preserve historical integrity at the moment the order occurred.

---

## 5. Data Governance & Quality SLA Matrix

| Layer | Check Category | Rule / Metric | Tooling | Threshold / Action |
| :--- | :--- | :--- | :--- | :--- |
| **Ingestion** | Schema Validation | Data Contract conformance (Pydantic) | Python Engine | 0% unhandled errors; non-conforming rows routed to DLQ |
| **Bronze** | Freshness SLA | Pipeline arrival delay <= 4 hours | Airflow Sensor | Alert Slack on SLA breach |
| **Bronze** | Completeness | Non-null audit columns (`_load_ts`, `_batch_id`) | Soda / dbt test | Fail pipeline if null |
| **Silver** | Uniqueness | Primary keys deduplicated via windowing | dbt test (`unique`) | Zero duplicate keys |
| **Gold** | Referential Integrity | FKs (`customer_sk`, `product_sk`, `date_sk`) | dbt test (`relationships`) | Zero orphan records (substitute with dummy unknown SKs) |
| **Gold** | Domain Range | Profit margin bounds, positive revenue | dbt test / Great Exp | Flag anomalies where `gross_margin_usd < -500` |

---

## 6. Orchestration & Backfill Strategy

### Airflow Pipeline Design
- **DAG ID:** `ecommerce_medallion_pipeline`
- **Schedule:** Daily at `03:00 UTC` (`0 3 * * *`)
- **Idempotency Guarantee:**
  - Tasks leverage deterministic parameters `{{ ds }}` (logical date) and `{{ run_id }}`.
  - Ingestion appends to bronze with audit identifiers.
  - Silver views deduplicate based on latest load timestamp.
  - Gold fact tables utilize dbt incremental logic (`merge` / `delete+insert`) using unique surrogate keys:
    ```sql
    {% if is_incremental() %}
        where _load_ts > (select max(_load_ts) from {{ this }})
    {% endif %}
    ```
- **Backfill Execution:** Historical dates can be re-run safely via `airflow dags backfill -s 2024-01-01 -e 2024-06-01 ecommerce_medallion_pipeline`. Because each run replaces/upserts data for that specific partition/key, no duplicate facts are created.
- **Failure Recovery:** Exponential backoff with 3 retries (2m, 4m, 8m).

---

## 7. Production Readiness Checklist

- [x] **Defensive Ingestion:** Pydantic schemas validating all inputs; automatic quarantine of corrupt payloads.
- [x] **Audit Lineage:** Every row carries `_load_ts`, `_batch_id`, `_source_file`, and `_record_hash`.
- [x] **Idempotency:** Re-running the pipeline 10 times yields identical state without duplicate rows.
- [x] **SCD Type 2:** Historical customer modifications tracked with point-in-time accuracy.
- [x] **Automated Testing:** dbt schema tests (`unique`, `not_null`, `accepted_values`, `relationships`) and pytest unit tests.
- [x] **Containerized Infrastructure:** One-command deployment via Docker Compose with healthchecks.
- [x] **Secrets Management:** Environment variables isolated via `.env` and `.env.example`.
