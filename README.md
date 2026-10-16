# Enterprise E-Commerce Analytics Platform 🛒⚡

[![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.9.3-017CEE.svg?style=flat&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![dbt Core](https://img.shields.io/badge/dbt-1.8-FF694B.svg?style=flat&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1.svg?style=flat&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED.svg?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C.svg?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org/)

An end-to-end, production-grade analytics platform built for an omnichannel e-commerce enterprise. The platform centralizes transactions, product catalogs, customer profiles, marketing ad spend, and user telemetry to compute mission-critical metrics: **Customer Lifetime Value (LTV)**, **Customer Acquisition Cost (CAC)**, **Average Order Value (AOV)**, and **Product Profit Margins**.

Follows an industry-standard **Medallion Architecture (Bronze -> Silver -> Gold)** with defensive data contracts, audit lineage tracking, SCD Type 2 dimension versioning, and fully idempotent ELT execution.

---

## 🏛️ Architecture Overview

```
                                      ARCHITECTURE DIAGRAM
                                      
  [Transactional OLTP]        [Ad Network APIs]        [Product Catalog]        [Web Telemetry]
     (Orders / Items)         (Meta / Google Ads)            (SKUs)              (Clickstream)
            │                         │                         │                      │
            └────────────┬────────────┴────────────┬────────────┘                      │
                         │                         │                                   │
                         ▼                         ▼                                   ▼
          ┌─────────────────────────────────────────────────────────────────────────────────┐
          │                  INGESTION ENGINE & DEFENSIVE DATA CONTRACTS                    │
          │  - Pydantic V2 Contract Validation (Types, Ranges, Status Enums)                │
          │  - Quarantine Dead-Letter-Queue (DLQ) for non-conforming rows                   │
          │  - Audit Enrichment: _load_ts, _batch_id, _source_file, _record_hash (SHA-256) │
          └──────────────────────────────────────┬──────────────────────────────────────────┘
                                                 │
                                                 ▼
          ┌─────────────────────────────────────────────────────────────────────────────────┐
          │                          BRONZE LAYER (RAW & AUDITED)                           │
          │  - MinIO Lakehouse: Partitioned Snappy Parquet (year=YYYY/month=MM/day=DD)      │
          │  - PostgreSQL Staging: bronze schema tables (append-only)                       │
          └──────────────────────────────────────┬──────────────────────────────────────────┘
                                                 │
                                                 ▼  dbt Staging Models & Snapshots
          ┌─────────────────────────────────────────────────────────────────────────────────┐
          │                        SILVER LAYER (CLEANED & ENRICHED)                        │
          │  - Deduplication via row_number() window functions                              │
          │  - Multi-Currency Conversion to USD via custom dbt macro                        │
          │  - SCD Type 2 tracking on customer attributes (valid_from, valid_to, is_current)│
          └──────────────────────────────────────┬──────────────────────────────────────────┘
                                                 │
                                                 ▼  dbt Marts & Incremental Modeling
          ┌─────────────────────────────────────────────────────────────────────────────────┐
          │                        GOLD LAYER (CURATED STAR SCHEMA)                         │
          │  - fact_orders: Incremental order grain with customer point-in-time SCD2 join   │
          │  - fact_order_items: Line-item grain with calculated net profit margin          │
          │  - dim_customer: Historical customer profiles with dynamic LTV tiers            │
          │  - dim_product: Product catalog with margin categorization                      │
          │  - dim_marketing_channel: Campaign efficiency, CTR, CPC, CPA metrics           │
          │  - dim_date: Conformed calendar dimension (2022-2030)                           │
          └──────────────────────────────────────┬──────────────────────────────────────────┘
                                                 │
                                                 ▼
          ┌─────────────────────────────────────────────────────────────────────────────────┐
          │                            BI & ANALYTICS SERVING                               │
          │             [Power BI]          [Metabase]          [Executive KPIs]            │
          └─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack & Engineering Rationale

| Component | Technology | Rationale |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 2.9 (Dockerized) | Industry standard for complex Directed Acyclic Graphs, automated retries with exponential backoffs, and deterministic backfills. |
| **Contract Validation** | Pydantic V2 | Sub-millisecond type coercion, field constraints, and automatic quarantine of corrupt payloads. |
| **Storage & Lakehouse** | MinIO + PostgreSQL 16 | Local S3-compatible object store using Snappy Parquet alongside Postgres for relational staging and marts. |
| **Transformations** | dbt Core 1.8 | Modular SQL transformations, lineage graphs, incremental merges, and automated testing suites. |
| **Dimensional Modeling** | Kimball Star Schema | Optimized for analytical BI querying with surrogate keys (`*_sk`) and SCD Type 2 tracking. |
| **Data Quality** | dbt Tests + Pytest | Uniqueness, nullability checks, accepted values, and foreign key referential integrity constraints. |

---

## 📁 Repository Directory Structure

```
├── .env.example                  # Environment variable template
├── .gitignore                    # Comprehensive ignore patterns
├── Dockerfile.airflow            # Custom Airflow image with JRE and data tools
├── docker-compose.yml            # Airflow, PostgreSQL Warehouse, MinIO stack
├── Makefile                      # CLI shortcuts for platform lifecycle
├── requirements.txt              # Pinned Python dependencies
├── config/                       # Airflow configurations
├── dags/
│   └── ecommerce_medallion_dag.py # Production Airflow Medallion DAG
├── data/
│   ├── raw/                      # Incoming sample datasets
│   ├── bronze/                   # Local lakehouse Parquet files
│   ├── quarantine/               # Dead-letter-queue for rejected rows
│   └── gold/
├── dbt/
│   ├── dbt_project.yml           # dbt project root
│   ├── profiles.yml              # Database connection profiles
│   ├── packages.yml              # External dbt packages (dbt_utils)
│   ├── macros/
│   │   ├── generate_surrogate_key.sql # MD5 surrogate key generator
│   │   └── currency_converter.sql     # Multi-currency to USD macro
│   ├── snapshots/
│   │   └── snap_customers_scd2.sql    # Customer SCD Type 2 snapshot
│   └── models/
│       ├── staging/              # Silver staging models
│       │   ├── src_ecommerce.yml
│       │   ├── stg_orders.sql
│       │   ├── stg_order_items.sql
│       │   ├── stg_customers.sql
│       │   ├── stg_products.sql
│       │   └── stg_marketing_spend.sql
│       └── marts/                # Gold star schema marts
│           ├── schema.yml        # Tests and descriptions
│           ├── dim_date.sql
│           ├── dim_customer.sql
│           ├── dim_product.sql
│           ├── dim_marketing_channel.sql
│           ├── fact_orders.sql
│           └── fact_order_items.sql
├── docker/
│   └── init-warehouse.sql        # Database initialization script
├── docs/
│   ├── ARCHITECTURE_AND_PRD.md   # Complete Technical Design Document & PRD
│   └── LINKEDIN_PORTFOLIO_POST.md# High-engagement showcase post
├── src/
│   ├── utils/
│   │   ├── logger.py             # Contextual JSON logger
│   │   └── audit.py              # Audit columns & row hash generator
│   └── ingestion/
│       ├── schemas.py            # Pydantic V2 data contracts
│       ├── file_ingestion.py     # File reader with DLQ quarantine
│       └── api_ingestion.py      # Resilient API extractor with retries
└── tests/
    ├── test_schemas.py           # Ingestion contract unit tests
    └── test_audit.py             # Audit column & hashing unit tests
```

---

## 🚀 Quickstart & Reproduction Guide

### Prerequisites
- Docker Engine & Docker Compose (v2.20+)
- Python 3.11+ (for local CLI running)

### 1. Initialize Configuration
```bash
cp .env.example .env
```

### 2. Launch the Platform Containers
Spin up PostgreSQL 16 (Analytical Warehouse), MinIO S3 Lakehouse, and Apache Airflow:
```bash
docker compose up -d --build
```
Verify all services are healthy:
```bash
docker compose ps
```
- **Airflow Web UI:** [http://localhost:8080](http://localhost:8080) (User: `admin` | Password: `admin`)
- **MinIO Console:** [http://localhost:9001](http://localhost:9001) (User: `minioadmin` | Password: `minioadmin_secure_key`)
- **Postgres Warehouse:** `localhost:5432` (`ecommerce_warehouse`)

### 3. Run Ingestion & Unit Tests Locally
```bash
# Run contract and audit unit tests
python -m pytest tests/ -v
```

### 4. Execute dbt Transformations
```bash
cd dbt

# 1. Execute customer SCD Type 2 snapshot
dbt snapshot --profiles-dir .

# 2. Run Silver staging views and execute quality tests
dbt run --select staging.* --profiles-dir .
dbt test --select staging.* --profiles-dir .

# 3. Build Gold dimensional star schema and test referential integrity
dbt run --select marts.* --profiles-dir .
dbt test --select marts.* --profiles-dir .
```

---

## 🧠 Engineering Challenges Solved

### 1. Point-in-Time Joins with SCD Type 2 Customers
*Challenge:* When customers move cities or change tiers, updating dimension rows in place overwrites history and invalidates prior geographic revenue attribution.  
*Solution:* Implemented `valid_from` and `valid_to` timestamps in `dim_customer`. `fact_orders` joins using non-equi join conditions (`order_timestamp >= valid_from AND order_timestamp < valid_to`), ensuring transactions are attributed to customer states at the exact moment of order placement.

### 2. Idempotency & Backfill Safety
*Challenge:* Scheduled pipelines frequently fail midway or require historical backfills, risking duplicated orders in analytical marts.  
*Solution:* 
- Silver staging models use deterministic deduplication: `row_number() over (partition by order_id order by order_timestamp desc, _load_ts desc) = 1`.
- Gold fact tables utilize dbt's `incremental` materialization with unique surrogate keys generated via deterministic MD5 hashing (`order_sk = md5(order_id)`).

### 3. Defensive Quarantine (Dead-Letter-Queue)
*Challenge:* Upstream schema drift and corrupt records crashing batch pipelines.  
*Solution:* Every raw batch passes through Pydantic V2 contracts. Invalid records are automatically written to a quarantine directory with explicit validation error metadata without blocking clean records from advancing.

---

## 📊 Core Business Metrics Generated

| Metric | Grain | Formula / Logic | Business Value |
| :--- | :--- | :--- | :--- |
| **Customer LTV** | Customer | Sum of completed order totals per customer across lifetime | Identifies high-value accounts (VIP / Platinum >= $5,000) for retention perks. |
| **CAC & ROAS** | Marketing Channel | `spend_amount / conversions` & `attributed_revenue / spend` | Reallocates ad budget away from underperforming channels. |
| **Gross Profit Margin** | Product & Order Line | `(net_subtotal_usd - total_cost_usd) / net_subtotal_usd` | Identifies margin compression and discounting leakage. |
| **AOV** | Cohort / Date | `total_amount_usd / count(orders)` | Measures order basket size trends across marketing promotions. |
