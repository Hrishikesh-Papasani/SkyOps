# SkyOps — Flight Operations Intelligence Platform

> **Business Question:** For US domestic airline operations, which flight delays are within the airline's control — and what is the compounding cost of those failures as delays cascade through the daily flying programme?

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                       │
│  BTS On-Time Performance (Kaggle, 29M rows, 2019–2023)                     │
│  NOAA IEM METAR Weather (Iowa State, 15 airports, hourly)                  │
│  FAA Airport 5010 Data (static reference)                                   │
└───────────────────────┬─────────────────────────────────────────────────────┘
                        │ Python ingestion scripts
                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS VOLUMES (raw landing)                         │
│  /Volumes/skyops/raw/bts/YYYY/    /Volumes/skyops/raw/metar/STATION/YYYY/  │
└───────────────────────┬─────────────────────────────────────────────────────┘
                        │ File arrival trigger → Lakeflow Job 1
                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              BRONZE LAYER (skyops.bronze) — Delta Lake                      │
│  skyops.bronze.flights        skyops.bronze.weather                         │
│  Explicit PySpark schema      Explicit schema                               │
│  Audit columns: _ingested_at, _source_file, _pipeline_run_id, _row_hash    │
│  Partitioned: year / month    Partitioned: station / year                   │
└───────────────────────┬─────────────────────────────────────────────────────┘
                        │ Lakeflow Job 2 (triggered by Job 1)
                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              SILVER LAYER (skyops.silver) — Business Rules                  │
│  Step 1: Separate CANCELLED=1 → silver.cancellations                       │
│  Step 2: Flag diverted flights                                               │
│  Step 3: Delay component reconciliation → silver.quarantine                 │
│  Step 4: Impossible value rejection → silver.quarantine                     │
│  Step 5: Computed columns (controllable_delay_minutes, etc.)                │
│  Step 6: METAR join (UTC-normalised via dim_airport_timezone)               │
│  Great Expectations checkpoint — blocks Job 3 if quarantine_rate > 0.5%   │
└───────────────────────┬─────────────────────────────────────────────────────┘
                        │ Lakeflow Job 3 (triggered by Job 2, GE passed)
                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              GOLD LAYER (skyops.gold) — dbt on Databricks SQL               │
│  fact_flights (incremental, merge, 35-day lookback)                         │
│  dim_airline (SCD Type 2 snapshot — captures merger transitions)            │
│  dim_airport (FAA 5010 + timezone seed)                                     │
│  dim_date (2019–2023, US holidays, COVID period, peak travel flags)         │
│  dim_weather_condition (VFR/MVFR/IFR/LIFR from METAR vsby)                 │
│  fct_delay_cascade (self-join, airline × route × consecutive flight)        │
└───────────────────────┬─────────────────────────────────────────────────────┘
                        │
              ┌─────────┴──────────┐
              ▼                    ▼
┌─────────────────────┐  ┌─────────────────────────────────────────────────┐
│  CONFLUENT CLOUD    │  │              POWER BI DESKTOP                   │
│  (Streaming Path)   │  │  Get Data → Databricks SQL Warehouse            │
│  flights.departures │  │  Import mode, refresh after each Gold run       │
│  (6 partitions)     │  │  Page 1: Operations Overview                    │
│  DLQ topic          │  │  Page 2: Airline Benchmarking                   │
│  Avro + Schema Reg. │  │  Page 3: Route & Airport Intelligence           │
│  Spark Structured   │  │  Page 4: Weather Correlation                    │
│  Streaming →        │  │  + Pipeline Health page (quality_runs table)    │
│  streaming_agg      │  └─────────────────────────────────────────────────┘
└─────────────────────┘
```

---

## Four Business Questions

| # | Question | Primary Gold Model |
|---|----------|--------------------|
| 1 | What proportion of delay minutes are controllable vs uncontrollable? | `fact_flights` — `controllable_delay_ratio` |
| 2 | Which airlines, routes, airports are worst offenders for controllable delay? | `fact_flights` + `dim_airline` + `dim_airport` |
| 3 | How does a morning delay cascade into afternoon delays on the same aircraft? | `fct_delay_cascade` |
| 4 | What does 10% reduction in controllable delay mean for on-time performance? | `fact_flights` — what-if parameter in Power BI |

---

## Tech Stack

| Component | Tool | Rationale |
|-----------|------|-----------|
| Compute | Databricks (serverless notebooks) | Managed Spark, Unity Catalog lineage, no infra |
| Storage | Delta Lake + Unity Catalog | ACID transactions, time travel, schema enforcement |
| Raw landing | Databricks Volumes | Integrated with Unity Catalog, no external object store config |
| Transformation | PySpark (Bronze/Silver) | Column-level lineage, schema enforcement |
| Modelling | dbt Core (Databricks adapter) | SQL-first, testable, documented |
| Serving | Databricks SQL Warehouse | Direct Power BI connector, serverless |
| Orchestration | Lakeflow Jobs (Databricks Workflows) | Native event triggers, no Airflow infra |
| Data Quality | Great Expectations | Checkpoint-based, blocks downstream on failure |
| CI/CD | Databricks Asset Bundles | Jobs-as-code, git-driven deployment |
| Streaming | Confluent Cloud (managed Kafka) | No broker management, Schema Registry included |
| Serving UI | Power BI Desktop | Free, enterprise-grade, import mode for performance |

**Rejected alternatives (see ADRs):**
- Local Spark + Docker → replaced by Databricks serverless
- Apache Airflow → replaced by Lakeflow Jobs (zero infra overhead)
- DuckDB for Gold → replaced by Databricks SQL Warehouse (direct Power BI connector, no export step)
- Local Kafka → replaced by Confluent Cloud ($400 trial)

---

## Data Sources

### Primary: BTS On-Time Performance (2019–2023)
- **Source:** [Kaggle — patrickzel/flight-delay-and-cancellation-dataset-2019-2023](https://www.kaggle.com/datasets/patrickzel/flight-delay-and-cancellation-dataset-2019-2023)
- **Scale:** ~29 million rows across 5 annual CSV files
- **Download:** `kaggle datasets download -d patrickzel/flight-delay-and-cancellation-dataset-2019-2023`
- **Landing:** `/Volumes/skyops/raw/bts/YYYY/`
- **Critical:** `DELAY_DUE_*` columns only populated when `ARR_DELAY >= 15`. Null ≠ zero.

### Enrichment: NOAA IEM METAR Weather
- **Source:** Iowa State Mesonet — `https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py`
- **Coverage:** 15 major US airports, hourly observations, 2019–2023
- **Landing:** `/Volumes/skyops/raw/metar/STATION/YYYY.csv`
- **Join challenge:** METAR uses ICAO codes + UTC; BTS uses IATA codes + local time. Requires `dim_airport_timezone` seed for DST-safe join.

### Reference: FAA Airport 5010 Data
- **Source:** [faa.gov/airports/airport_safety/airportdata_5010](https://www.faa.gov/airports/airport_safety/airportdata_5010)
- **Landing:** `/Volumes/skyops/raw/faa/`
- **Used for:** `dim_airport` — hub classification, runway count, elevation, coordinates

---

## Quickstart (Databricks Asset Bundles)

### Prerequisites
1. Paid Databricks workspace (AWS `ap-south-1` or Azure `centralindia`)
2. Unity Catalog metastore with `skyops` catalog
3. Confluent Cloud cluster + API credentials
4. Kaggle API credentials

### Setup

```bash
# 1. Clone repository
git clone https://github.com/<your-handle>/skyops.git
cd skyops

# 2. Install Databricks CLI
pip install databricks-cli

# 3. Configure workspace connection
databricks configure --token

# 4. Create Secret Scope (one-time)
databricks secrets create-scope skyops-secrets
databricks secrets put --scope skyops-secrets --key confluent_api_key
databricks secrets put --scope skyops-secrets --key confluent_api_secret
databricks secrets put --scope skyops-secrets --key kaggle_username
databricks secrets put --scope skyops-secrets --key kaggle_key

# 5. Deploy all jobs
databricks bundle deploy

# 6. Validate configuration
databricks bundle validate
```

### Running the Pipeline

```bash
# Ingest + full pipeline (triggers all 3 jobs in sequence)
databricks bundle run skyops_monthly_ingest

# Run individual jobs
databricks bundle run skyops_silver_quality
databricks bundle run skyops_gold_refresh

# Streaming (always-on — start separately)
databricks bundle run skyops_streaming_consumer
```

### dbt (Gold layer)

```bash
cd dbt/
pip install dbt-databricks dbt-utils

# Set token
export DBT_TOKEN=<your-databricks-pat>

dbt deps
dbt seed                          # Load airport_timezone, airline_metadata, airport_metadata
dbt run --select +fact_flights+   # Run full Gold DAG
dbt test                          # Run all tests
dbt docs generate && dbt docs serve  # View lineage
```

---

## Repository Structure

```
skyops/
├── databricks.yml                  # Asset Bundle — all 3 jobs as code
├── README.md
│
├── notebooks/
│   ├── bronze/
│   │   ├── bronze_bts.py           # BTS CSV → skyops.bronze.flights
│   │   └── bronze_metar.py         # METAR CSV → skyops.bronze.weather
│   ├── silver/
│   │   ├── silver_flights.py       # 6-step business rules + METAR join
│   │   └── silver_weather.py       # METAR cleaning + standardisation
│   ├── streaming/
│   │   ├── kafka_producer.py       # Chronological BTS replay → Confluent
│   │   └── streaming_consumer.py   # Spark Structured Streaming + DLQ
│   └── quality/
│       ├── great_expectations_check.py
│       └── data_freshness_check.py
│
├── ingestion/
│   ├── bts_downloader.py           # Kaggle API → Databricks Volume
│   └── metar_downloader.py         # IEM API → Databricks Volume
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── seeds/
│   │   ├── airport_timezone.csv    # 15 rows: IATA, ICAO, tz string
│   │   ├── airport_metadata.csv    # FAA 5010 hub classification
│   │   └── airline_metadata.csv    # Carrier names, alliance, LCC flag
│   ├── snapshots/
│   │   └── dim_airline_snapshot.sql
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_flights.sql
│   │   │   └── stg_weather.sql
│   │   └── marts/
│   │       ├── fact_flights.sql
│   │       ├── dim_airline.sql
│   │       ├── dim_airport.sql
│   │       ├── dim_date.sql
│   │       ├── dim_weather_condition.sql
│   │       └── fct_delay_cascade.sql
│   └── tests/
│       ├── assert_controllable_delay_non_negative.sql
│       └── assert_delay_components_within_tolerance.sql
│
├── power_bi/
│   └── skyops_dashboard.pbix
│
├── docs/
│   ├── adr/
│   │   ├── ADR-001-databricks-over-local-spark.md
│   │   ├── ADR-002-lakeflow-jobs-over-airflow.md
│   │   ├── ADR-003-incremental-over-full-refresh.md
│   │   ├── ADR-004-import-vs-directquery-powerbi.md
│   │   └── ADR-005-confluent-cloud-over-local-kafka.md
│   ├── runbook.md
│   └── report/
│       ├── flight_ops_intelligence_report.md
│       ├── cloud_cost_analysis.md
│       └── charts/
│
└── resources/
    └── databricks_jobs/
```

---

## Key Metrics

| Metric | Definition | Source |
|--------|------------|--------|
| `on_time_rate` | % non-cancelled flights where `ARR_DELAY <= 14` | `fact_flights` |
| `controllable_delay_ratio` | `controllable_delay_minutes / total ARR_DELAY minutes` | `fact_flights` |
| `delay_cascade_rate` | % flights where `DELAY_DUE_LATE_AIRCRAFT > 0` | `fact_flights` |
| `airborne_recovery_rate` | % delayed departures where `DEP_DELAY > ARR_DELAY` | `fact_flights` |
| `block_time_padding_index` | `avg(CRS_ELAPSED_TIME - ELAPSED_TIME)` per route | `fact_flights` |
| `quarantine_rate` | `quarantine_count / bronze_count` (alert > 0.5%) | `quality_runs` |
| `data_freshness_days` | Days since `max(fl_date)` in `fact_flights` (alert > 40) | `quality_runs` |

---

## Seven Analytical Findings

1. **Late Aircraft = ~38% of all industry delay minutes** — the single largest controllable category
2. **Controllable delay ratio varies 2× between best and worst airlines** — structural, not random
3. **The 2pm cascade** — `DELAY_DUE_LATE_AIRCRAFT` spikes after 14:00 at ATL, ORD, DFW (heatmap)
4. **Visibility threshold** — non-linear delay increase below 3 miles; discovered only via METAR join
5. **Southwest December 2022** — identifiable as statistical anomaly spike in the data
6. **Route reliability** — std dev of `ARR_DELAY` reveals unreliable routes that mean-delay hides
7. **Airborne recovery** — airlines differ significantly in recovering departure delays in the air

---

## Cost Breakdown (10-Week Build)

| Component | Estimated Cost (USD) |
|-----------|----------------------|
| Databricks compute (serverless notebooks) | $15–25 |
| Databricks SQL Warehouse (dbt + Power BI) | $5–10 |
| Databricks storage (Unity Catalog + Delta) | $3–5 |
| Confluent Cloud (within $400 trial) | $0 |
| Power BI Desktop | $0 |
| **Total** | **$23–40 USD (~₹1,900–3,300)** |

See [docs/report/cloud_cost_analysis.md](docs/report/cloud_cost_analysis.md) for actual spend with billing screenshots.

---

## Build Phases

| Phase | Weeks | Deliverable |
|-------|-------|-------------|
| 1 — Cloud Setup | 1–2 | Workspace running, 2022 data in Unity Catalog, Confluent green |
| 2 — Bronze + Silver | 3–4 | silver.flights + silver.quarantine populated, Incident #1 documented |
| 3 — Gold + Streaming | 5–6 | dbt lineage graph, Confluent topic active, streaming_agg populating |
| 4 — Orchestration + Full Load | 7–8 | 29M rows in fact_flights, all 3 jobs tested including failure path |
| 5 — Power BI + Docs | 9–10 | .pbix committed, all ADRs + runbook + report complete, demo video linked |

---

## Demo Video

> [Link to be added after Phase 5]

Demo sequence: Confluent Cloud topic → trigger Job 1 → Job 2 → Job 3 → Power BI refresh → Incident #1 quarantine row in Unity Catalog

---

## Licence

See [LICENSE](LICENSE).
