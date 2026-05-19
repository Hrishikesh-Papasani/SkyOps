# SkyOps Medallion Architecture

SkyOps follows the **Medallion Architecture** pattern for data lakehouse organization, providing progressive layers of data refinement from raw ingestion to business-ready analytics.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      RAW DATA SOURCES                           │
│  BTS TranStats API │ Weather APIs │ Airport Data │ ...         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       BRONZE LAYER                              │
│  📂 /SkyOps/bronze/        📊 skyops.bronze.*                  │
│  • bts_ontime_ingest.py    • bts_ontime                        │
│                                                                  │
│  Raw ingestion, minimal transformation                          │
│  Schema enforcement, partitioning, data preservation            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       SILVER LAYER                              │
│  📂 /SkyOps/silver/        📊 skyops.silver.*                  │
│  • (future) bts_ontime_clean.py                                │
│  • (future) airport_reference.py                               │
│                                                                  │
│  Data quality, cleansing, standardization, enrichment           │
│  Type conversions, deduplication, SCD tracking                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                               │
│  📂 /SkyOps/gold/          📊 skyops.gold.*                    │
│  • (future) flight_performance_daily.py                        │
│  • (future) airline_scorecard.py                               │
│                                                                  │
│  Business-level aggregations, KPIs, ML features                 │
│  Optimized for BI tools, dashboards, reporting                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CONSUMPTION LAYER                            │
│  Dashboards │ BI Tools │ ML Models │ APIs │ Reports            │
└─────────────────────────────────────────────────────────────────┘
```

## Folder Structure

```
/SkyOps/
│
├── ingestion/                # Raw data fetching scripts
│   └── bts_downloader.py     # BTS API → CSV extractor
│
├── bronze/                   # Bronze layer notebooks
│   └── bts_ontime_ingest.py  # ✅ CSV → skyops.bronze.bts_ontime
│
├── silver/                   # Silver layer notebooks
│   ├── README.md             # Silver layer documentation
│   └── (future notebooks)    # Cleansing, enrichment
│
├── gold/                     # Gold layer notebooks
│   ├── README.md             # Gold layer documentation
│   └── (future notebooks)    # Aggregations, KPIs
│
├── ARCHITECTURE.md           # This file - medallion architecture docs
└── AGENTS.md                 # Agent instructions (existing)
```

## Layer Definitions

### Bronze Layer (Raw Ingestion)

**Purpose**: Ingest raw data with minimal transformation

**Characteristics**:
* Preserves original data fidelity (append-only or full refresh)
* Applies only essential transformations (schema enforcement, partitioning)
* Acts as a staging layer for recovery and reprocessing
* Data quality issues are NOT fixed here (deferred to Silver)

**Folder**: `/SkyOps/bronze/`  
**Catalog Schema**: `skyops.bronze`  
**Table Naming**: `skyops.bronze.<source>_<entity>`  
**Example**: `skyops.bronze.bts_ontime` (28M rows of raw BTS flight data)

**Implemented**:
* ✅ `bts_ontime_ingest.py` - BTS flight data (Feb 2022 - Feb 2026)

**Planned**:
* Weather data ingestion
* Airport reference data ingestion
* Airline reference data ingestion

---

### Silver Layer (Cleansed & Validated)

**Purpose**: Apply data quality rules and standardization

**Characteristics**:
* Fixes data quality issues (nulls, outliers, duplicates)
* Standardizes formats (dates, times, strings)
* Enriches with reference data (joins)
* Implements Slowly Changing Dimensions (SCD)
* Production-grade quality for analytics

**Folder**: `/SkyOps/silver/`  
**Catalog Schema**: `skyops.silver`  
**Table Naming**: `skyops.silver.<source>_<entity>`  
**Example**: `skyops.silver.bts_ontime` (cleaned flight data)

**Planned**:
* `bts_ontime_clean.py` - Date/time parsing, timezone handling, enrichment
* `airport_reference.py` - Airport dimension table (lat/lon, region)
* `airline_reference.py` - Airline dimension table (alliances, branding)

---

### Gold Layer (Business-Ready)

**Purpose**: Business-level aggregations and KPIs

**Characteristics**:
* Pre-aggregated for fast queries
* Implements business logic (KPIs, metrics, scorecards)
* Optimized for BI tools (Tableau, Power BI)
* ML feature stores for predictive models
* Star schema for dimensional modeling

**Folder**: `/SkyOps/gold/`  
**Catalog Schema**: `skyops.gold`  
**Table Naming**: `skyops.gold.<business_domain>_<granularity>`  
**Example**: `skyops.gold.flight_performance_daily`

**Planned**:
* `flight_performance_daily.py` - Daily on-time performance metrics
* `route_analytics.py` - Route-level statistics
* `airline_scorecard.py` - Airline performance rankings
* `ml_delay_features.py` - ML feature store for delay prediction

---

## Data Flow Example: BTS Flight Data

```
1. RAW SOURCE
   BTS TranStats API (49 months of data)
          │
          ▼
2. STAGING (not a medallion layer, but useful for resilience)
   /Volumes/skyops/bronze/bts/staging/*.csv
   - Monthly CSV files (resume-friendly)
   - 28,178,862 rows total
          │
          ▼
3. BRONZE LAYER (/SkyOps/bronze/bts_ontime_ingest.py)
   skyops.bronze.bts_ontime
   - Raw ingestion with schema enforcement
   - Partitioned by YEAR
   - FL_DATE as string (yyyyMMdd)
   - 34 columns (32 business + YEAR + MONTH)
          │
          ▼
4. SILVER LAYER (future: /SkyOps/silver/bts_ontime_clean.py)
   skyops.silver.bts_ontime
   - FL_DATE parsed to date type
   - Departure/arrival times converted to timestamps
   - Timezone handling (UTC conversions)
   - Enriched with airport lat/lon, region
   - Data quality rules applied
          │
          ▼
5. GOLD LAYER (future: /SkyOps/gold/flight_performance_daily.py)
   skyops.gold.flight_performance_daily
   - Daily aggregations by airline, airport, route
   - On-time performance metrics
   - Delay distributions
   - Pre-joined for fast BI queries
```

## Unity Catalog Organization

```
skyops (catalog)
├── bronze (schema)                 ← Bronze layer output
│   ├── bts (volume)                ← Staging CSVs stored here
│   │   └── staging/*.csv
│   └── bts_ontime (table)          ← Bronze Delta table
│
├── silver (schema)                 ← Silver layer output (future)
│   ├── bts_ontime (table)
│   ├── airport_dim (table)
│   └── airline_dim (table)
│
└── gold (schema)                   ← Gold layer output (future)
    ├── flight_performance_daily (table)
    ├── route_analytics (table)
    └── airline_scorecard (table)
```

## Design Principles

### 1. **Separation of Concerns**
* Each layer has a single, well-defined responsibility
* Bronze preserves raw data, Silver cleanses, Gold aggregates

### 2. **Idempotency**
* Re-running notebooks produces the same result
* Enables safe retries and backfills

### 3. **Incremental Processing**
* Process only new/changed data when possible (Delta merge/CDC)
* Full refresh when necessary (small dimensions, historical corrections)

### 4. **Data Quality by Layer**
* Bronze: No quality checks (accept all data)
* Silver: Enforce quality rules, log violations
* Gold: Assume high quality, optimize for performance

### 5. **Schema Evolution**
* Use `.option("mergeSchema", "true")` for additive changes
* Plan schema changes across all dependent layers
* Version schemas when breaking changes are necessary

### 6. **Auditing & Lineage**
* Preserve staging data for audit trails
* Use Delta time travel for historical queries
* Document transformations in notebook markdown cells

## Current Status

| Layer  | Status | Tables | Notebooks |
|--------|--------|--------|----------|
| Bronze | ✅ Complete | 1 | 1 |
| Silver | 🔲 Planned | 0 | 0 |
| Gold   | 🔲 Planned | 0 | 0 |

### Completed
* ✅ Bronze: `skyops.bronze.bts_ontime` (28.2M rows, Feb 2022 - Feb 2026)
* ✅ Folder structure: bronze/, silver/, gold/
* ✅ Documentation: README files in each layer

### Next Steps
1. Implement Silver layer: `bts_ontime_clean.py`
2. Create airport/airline reference tables
3. Implement Gold layer: `flight_performance_daily.py`
4. Set up automated refresh schedules (Databricks Jobs)
5. Create BI dashboards consuming Gold tables

## Development Workflow

1. **Add new data source**:
   - Create ingestion script in `/ingestion/`
   - Create Bronze notebook in `/bronze/`
   - Update this ARCHITECTURE.md

2. **Develop transformation**:
   - Start in Silver or Gold folder
   - Follow naming conventions
   - Include validation cells
   - Document business logic

3. **Production deployment**:
   - Create Databricks Job for scheduled runs
   - Set up monitoring/alerting
   - Document SLAs and dependencies

## References

* [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
* [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)
* [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)