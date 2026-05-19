# SkyOps — Agent Context & Handoff

> **Read this first.** This file is the authoritative source of truth for any AI agent
> or developer continuing work on this repository. It is intentionally comprehensive.

---

## 1. Project Overview

**SkyOps Flight Operations Intelligence Platform** — a portfolio data-engineering project
that ingests 7+ years of US domestic on-time performance data (Jan 2019 - Feb 2026) from the Bureau of
Transportation Statistics (BTS), enriches it with METAR weather observations, and
delivers analytical Gold-layer models to Power BI.

**Four business questions this platform answers:**
1. Which routes have the worst on-time performance, and why?
2. How do weather conditions correlate with delay severity by airport?
3. Which carriers recover fastest from initial delays?
4. How did COVID-19 reshape the US domestic network (2020-2022)?

---

## 2. Repository Structure

```
SkyOps/
├── AGENTS.md                  ← YOU ARE HERE — agent handoff document
├── README.md                  ← Full architecture overview for humans
├── LICENSE
├── .gitignore
├── requirements.txt           ← All Python dependencies
├── databricks.yml             ← Databricks Asset Bundle (4 Lakeflow Jobs)
│
├── ingestion/
│   └── bts_downloader.py      ← ✅ COMPLETE — BTS TranStats downloader
│   (metar_downloader.py)      ← ❌ NOT YET BUILT
│
├── dbt/
│   ├── dbt_project.yml        ← Project config (profile=skyops, catalog=skyops)
│   ├── profiles.yml           ← Databricks SQL Warehouse connection
│   ├── packages.yml           ← dbt-utils, dbt_databricks_utils
│   └── seeds/
│       ├── airport_timezone.csv   ← 15 airports: IATA→ICAO→timezone_str (CRITICAL for METAR join)
│       ├── airport_metadata.csv   ← 30 airports with hub class, coords, runway count
│       └── airline_metadata.csv  ← 15 carriers with LCC flag, alliance, DOT code
│   (models/)                  ← ❌ NOT YET BUILT (staging/, marts/ subdirs needed)
│
├── docs/
│   ├── runbook.md             ← 4 fully documented incidents + debug checklists
│   ├── adr/
│   │   ├── ADR-001.md         ← Databricks Serverless over Local Spark
│   │   ├── ADR-002.md         ← Lakeflow Jobs over Airflow
│   │   ├── ADR-003.md         ← Incremental model for fact_flights
│   │   ├── ADR-004.md         ← Power BI Import mode over DirectQuery
│   │   └── ADR-005.md         ← Confluent Cloud over Local Kafka
│   └── report/
│       ├── flight_ops_intelligence_report.md
│       └── cloud_cost_analysis.md
│
└── data/                      ← GITIGNORED — local only
    └── raw/bts/
        └── staging/           ← Monthly CSVs written by bts_downloader.py
```

---

## 3. What Is Complete

| Component | Status | Notes |
|-----------|--------|-------|
| `ingestion/bts_downloader.py` | ✅ Complete & tested | Downloads 32-field BTS data, verified across full date range |
| `requirements.txt` | ✅ Complete | All dependencies declared |
| `databricks.yml` | ✅ Complete | 4 Lakeflow Jobs as Asset Bundle |
| `dbt/` config files | ✅ Complete | dbt_project.yml, profiles.yml, packages.yml |
| `dbt/seeds/` | ✅ Complete | 3 seed files (timezone, airport, airline metadata) |
| `docs/runbook.md` | ✅ Complete | 4 incidents documented |
| `docs/adr/` | ✅ Complete | 5 ADRs |
| `README.md` | ✅ Complete | Full architecture + quickstart |
| `.gitignore` | ✅ Complete | data/, dbt artefacts, secrets excluded |
| Bronze Delta table | ✅ Complete | skyops.bronze.bts_ontime — 46.8M rows, Jan 2019 - Feb 2026 |

---

## 4. What Is NOT Yet Built

| Component | Priority | Notes |
|-----------|----------|-------|
| `ingestion/metar_downloader.py` | HIGH | IEM ASOS API, 15 airports, Jan 2019 - Feb 2026 |
| `dbt/models/staging/` | HIGH | STG models for Bronze→Silver transformation |
| `dbt/models/marts/` | HIGH | dim_airport, dim_airline, dim_date, fct_flights, fct_delay_cascade |
| Databricks notebooks | MEDIUM | Bronze ingestion PySpark notebook (reads from Volumes) |
| Great Expectations suite | MEDIUM | Data quality checkpoints |
| Kafka producer | LOW | Structured Streaming path (optional for portfolio) |

---

## 5. Technology Stack

| Layer | Technology | Key Detail |
|-------|-----------|------------|
| Ingestion (local) | Python 3.13 + requests + BS4 | `ingestion/bts_downloader.py` |
| Storage (cloud) | Databricks Unity Catalog | Catalog: `skyops`, schemas: `bronze`/`silver`/`gold` |
| Raw landing | Unity Catalog Volumes | `/Volumes/skyops/bronze/bts/staging/` |
| Transform | dbt Core + Databricks adapter | SQL Warehouse, `dbt run --target prod` |
| Orchestration | Databricks Lakeflow Jobs | 4 jobs in `databricks.yml` |
| Streaming | Confluent Cloud Kafka | Topic: `flights.departures` (6 partitions) |
| BI | Power BI Desktop | Import mode, connects to Databricks SQL Warehouse |
| Quality | Great Expectations 0.18 | Checkpoints in silver job |

---

## 6. BTS Downloader — Critical Technical Details

### What it does
Downloads BTS On-Time Reporting data from:
`https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=N8vn6v10%20f722146%20gnoyr5`

The site is an ASP.NET WebForms application. The script:
1. GETs the page to capture `__VIEWSTATE` + `__EVENTVALIDATION` tokens
2. POSTs a year-change `__EVENTTARGET=cboYear` postback (required for years ≠ latest year)
3. POSTs the download form with 32 selected checkbox fields
4. Receives a ZIP containing `T_ONTIME_REPORTING.csv`
5. Normalises `FL_DATE` from `"M/D/YYYY 12:00:00 AM"` → `"yyyyMMdd"` string

### 32 Selected Fields (FORM_CHECKBOXES internal names → OUTPUT column names)
| BTS Internal Name | Output Column |
|-------------------|---------------|
| FL_DATE | FL_DATE |
| OP_UNIQUE_CARRIER | AIRLINE_CODE |
| OP_CARRIER_AIRLINE_ID | DOT_CODE |
| OP_CARRIER_FL_NUM | FL_NUMBER |
| ORIGIN_AIRPORT_ID | OriginAirportID |
| DEST_AIRPORT_ID | DestAirportID |
| ORIGIN_CITY_NAME | ORIGIN_CITY |
| DEST | DEST |
| DEST_CITY_NAME | DEST_CITY |
| CRS_DEP_TIME | CRS_DEP_TIME |
| DEP_TIME | DEP_TIME |
| DEP_DELAY | DEP_DELAY |
| TAXI_OUT | TAXI_OUT |
| WHEELS_OFF | WHEELS_OFF |
| WHEELS_ON | WHEELS_ON |
| TAXI_IN | TAXI_IN |
| CRS_ARR_TIME | CRS_ARR_TIME |
| ARR_TIME | ARR_TIME |
| ARR_DELAY | ARR_DELAY |
| CANCELLED | CANCELLED |
| CANCELLATION_CODE | CANCELLATION_CODE |
| DIVERTED | DIVERTED |
| CRS_ELAPSED_TIME | CRS_ELAPSED_TIME |
| ACTUAL_ELAPSED_TIME | ELAPSED_TIME |
| AIR_TIME | AIR_TIME |
| DISTANCE | DISTANCE |
| CARRIER_DELAY | DELAY_DUE_CARRIER |
| WEATHER_DELAY | DELAY_DUE_WEATHER |
| NAS_DELAY | DELAY_DUE_NAS |
| SECURITY_DELAY | DELAY_DUE_SECURITY |
| LATE_AIRCRAFT_DELAY | DELAY_DUE_LATE_AIRCRAFT |
| TAIL_NUM | Tail_Number |

### Key constants
```python
REQUEST_DELAY_SECONDS = 3      # Be respectful to BTS servers
MAX_RETRIES = 3
RETRY_BACKOFF_BASE_SECONDS = 15
```

### CLI Usage
```bash
# Single month test
python ingestion/bts_downloader.py --month 2024-01 --output-dir /Volumes/skyops/bronze/bts

# Download specific date range (e.g., backfill Jan 2019 - Jan 2022)
python ingestion/bts_downloader.py --month 2019-01 --output-dir /Volumes/skyops/bronze/bts
# ... repeat for each month or use a loop in the notebook

# Resume interrupted download (skips already-downloaded months automatically)
python ingestion/bts_downloader.py --month 2024-06 --output-dir /Volumes/skyops/bronze/bts

# Force re-download a specific month
python ingestion/bts_downloader.py --month 2024-06 --output-dir /Volumes/skyops/bronze/bts --force
```

### Output
- **Staging CSVs**: `/Volumes/skyops/bronze/bts/staging/YYYY_MM.csv` (one per month)
- **Total coverage**: 86 months (Jan 2019 - Feb 2026)
- **Total rows**: ~46.8M flights

---

## 7. METAR Downloader — Not Yet Built

When building `ingestion/metar_downloader.py`, use the IEM ASOS API:

```
https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py
  ?station=KATL&year1=2019&month1=1&day1=1
  &year2=2026&month2=2&day2=28
  &data=tmpf,vsby,sknt,p01i,wxcodes&tz=UTC&format=comma&latlon=no&direct=yes
```

**15 target airports (ICAO codes)**:
KATL, KORD, KDEN, KDFW, KLAX, KJFK, KSFO, KLAS, KPHX, KMIA, KSEA, KMSP, KBOS, KDTW, KEWR

The timezone mapping for METAR→local join is in `dbt/seeds/airport_timezone.csv`.
**Critical**: Use `UTC` for API requests, convert to local time in dbt using the seed file.

**Date range**: Jan 1, 2019 - Feb 28, 2026 (match BTS coverage)

---

## 8. Databricks Schema & Data Flow

```
Local Script          Databricks Unity Catalog
──────────            ────────────────────────────────────────────────
bts_downloader.py ──► /Volumes/skyops/bronze/bts/staging/*.csv   (Raw CSVs)
                       ↓  (Notebook: bronze/bts_ontime_ingest.py)
                      skyops.bronze.bts_ontime                    (Delta table, 46.8M rows)
                       ↓  (Lakeflow Job: skyops_silver_quality)
                      skyops.silver.fct_flights_clean             (GE validated)
                       ↓  (Lakeflow Job: skyops_gold_refresh)
                      skyops.gold.fct_flights                     (dbt incremental)
                      skyops.gold.dim_airport
                      skyops.gold.dim_airline
                      skyops.gold.dim_date
                       ↓
                      Power BI (Import mode, SQL Warehouse)
```

**Data Coverage**:
- **Date range**: Jan 1, 2019 - Feb 28, 2026 (86 months)
- **Total flights**: 46,822,552 rows
- **Years covered**: 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 (partial)
- **COVID impact visible**: 2020 shows 37% reduction vs 2019

---

## 9. Environment Variables Required

```bash
# Databricks
DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
DBT_TOKEN=<databricks-personal-access-token>

# dbt profiles.yml reads these at runtime
# Do NOT hardcode credentials in any file
```

---

## 10. Known Issues & Gotchas

1. **Windows UTF-8 logging**: `bts_downloader.py` wraps stdout in `io.TextIOWrapper(encoding="utf-8")` to avoid `UnicodeEncodeError` on cp1252 Windows consoles. Do not remove this.

2. **ASP.NET PostBack requirement**: For any year that is NOT the latest available year, the downloader must POST a year-change postback (`__EVENTTARGET=cboYear`) BEFORE the actual download POST. Without this, BTS returns an EventValidation error. The `refresh_year_state()` function handles this.

3. **FL_DATE format**: BTS returns `"1/1/2024 12:00:00 AM"` not `"20240101"`. The `normalise_fl_date()` function converts this. The Bronze PySpark schema expects `yyyyMMdd` string format.

4. **Resume capability**: The downloader skips months where the staging CSV already exists. Use `--force` to re-download a specific month.

5. **dbt incremental model**: `fct_flights` uses `unique_key='flight_id'` with a 35-day lookback window (`incremental_lookback_days=35` variable in dbt_project.yml) to handle late-arriving data corrections from BTS.

6. **METAR DST issue** (Incident #002 in runbook): METAR data is in UTC. The airport_timezone seed has both `utc_offset_standard` and `utc_offset_dst` columns. PHX correctly has UTC-7 for both (Arizona has no DST).

7. **Databricks Volumes path**: When running the downloader output on Databricks, `--output-dir` must point to a DBFS or Volumes path, e.g., `/Volumes/skyops/bronze/bts`. The `sys.stdout.buffer` wrapper in `setup_logging()` may fail on Databricks (no buffer attribute). Wrap the setup_logging call in a try/except that falls back to `sys.stdout` directly.

8. **BTS website changes**: The BTS TranStats website occasionally changes its layout. If the script fails with "Could not find 'Latest Available Data'", use the `--month` argument to specify exact months instead of relying on `--years` auto-detection.

---

## 11. dbt Models — What Needs To Be Built

### Staging layer (`skyops.silver`, materialized as `view`)
- `stg_bts_flights.sql` — select + cast from `skyops.bronze.bts_ontime`
- `stg_metar_obs.sql` — select + cast from `skyops.bronze.metar_raw`

### Marts layer (`skyops.gold`, materialized as `table` or `incremental`)
- `dim_date.sql` — date spine from 2019-01-01 to 2026-12-31
- `dim_airport.sql` — from `ref('airport_metadata')` seed
- `dim_airline.sql` — from `ref('airline_metadata')` seed
- `fct_flights.sql` — incremental, joins stg_bts_flights + stg_metar_obs, unique_key='flight_id'
- `fct_delay_cascade.sql` — self-join on TAIL_NUM + date to find propagated delays

### dbt variables in scope (set in dbt_project.yml)
```yaml
dim_date_start: "2019-01-01"
dim_date_end: "2026-12-31"
delay_reporting_threshold_minutes: 15
delay_reconciliation_tolerance_minutes: 2
incremental_lookback_days: 35
max_freshness_days: 40
```

---

## 12. Suggested Next Steps (Priority Order)

1. **Build METAR downloader** (`ingestion/metar_downloader.py`) — IEM ASOS API, Jan 2019 - Feb 2026.

2. **Deploy Asset Bundle** to Databricks:
   ```bash
   databricks bundle deploy --target prod
   ```

3. **Build dbt models** (staging → marts) following schema in section 11 above.

4. **Connect Power BI** to Databricks SQL Warehouse and build dashboards for the 4 business questions.

5. **Set up incremental refresh**: Configure Lakeflow Jobs to run monthly on new data arrivals.