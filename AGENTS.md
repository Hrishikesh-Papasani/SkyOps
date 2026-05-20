# SkyOps — Agent Context & Handoff

> **Read this first.** This file is the authoritative source of truth for any AI agent
> or developer continuing work on this repository. It is intentionally comprehensive.

---

## 1. Project Overview

**SkyOps Flight Operations Intelligence Platform** — a portfolio data-engineering project
that ingests 7+ years of US domestic on-time performance data (Jan 2019 - Feb 2026) from the Bureau of
Transportation Statistics (BTS), enriches it with METAR weather observations, and
delivers analytical Gold-layer models to Power BI.

**Five problem domains this platform answers:**
1. **Operations** — Where are we losing time and is it our fault? (controllable vs uncontrollable delay split, airborne recovery, taxi efficiency)
2. **Route Planning** — Which routes and schedules are structurally broken? (block time padding, cascade by hour, route delay variance)
3. **Airport Operations** — Which airports are systemic delay generators? (origin as injector, destination as amplifier, taxi outliers)
4. **Fleet & Turnaround** — Is our aircraft utilisation efficient? (turnaround proxy via Tail_Number chain, cascade reconstruction)
5. **Schedule Integrity** — How reliable is our published schedule? (cancellation patterns, Southwest Dec 2022 anomaly, diversion rates)

**Iteration scope:** Iteration 1 uses BTS On-Time data only. METAR weather enrichment is deferred to Iteration 2.

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
│   (seeds/)                   ← ❌ NOT YET BUILT
│   │   airport_metadata.csv   ← 30 airports: bts_airport_id (PK), IATA, hub class, coords
│   │   airline_metadata.csv   ← 15 carriers: LCC flag, alliance, DOT code
│   │   airport_timezone.csv   ← 15 airports: IATA→ICAO→timezone_str (for Iteration 2 METAR join)
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
| `dbt/seeds/` | ❌ Not built | CSV files do not exist on disk yet — directory is absent |
| `docs/runbook.md` | ✅ Complete | 4 incidents documented |
| `docs/adr/` | ✅ Complete | 6 ADRs (ADR-006 added: star schema design) |
| `README.md` | ✅ Complete | Full architecture + quickstart |
| `.gitignore` | ✅ Complete | data/, dbt artefacts, secrets excluded |
| Bronze Delta table | ✅ Complete | skyops.bronze.bts_ontime — 52.8M rows, Jan 2019 - Feb 2026 (verified via EDA) |

---

## 4. What Is NOT Yet Built

| Component | Priority | Notes |
|-----------|----------|-------|
| `dbt/seeds/` CSV files | HIGH | airport_metadata (needs bts_airport_id col), airline_metadata, airport_timezone |
| `dbt/models/staging/stg_bts_flights.sql` | HIGH | Cast, compute dep_time_minutes, LPAD CRS_DEP_TIME, build flight_id |
| `dbt/models/marts/` | HIGH | dim_date, dim_airport, dim_airline, dim_aircraft, fct_flights, fct_delay_cascade |
| Aircraft dimension table | HIGH | Tail_Number → aircraft type/family; user will source and load separately |
| Databricks Silver notebooks | HIGH | silver_flights.py (6 steps), cancelled-first rule (Incident #004) |
| Great Expectations suite | MEDIUM | Data quality checkpoints |
| `ingestion/metar_downloader.py` | LOW (Iteration 2) | IEM ASOS API, 15 airports — deferred until BTS-only iteration is complete |
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
                       ↓  (Notebook: bronze/bts_ontime_ingest.ipynb)
                      skyops.bronze.bts_ontime                    (Delta table, 52.8M rows)
                       ↓  (Lakeflow Job: skyops_silver_quality)
                      skyops.silver.flights                       (non-cancelled, GE validated)
                      skyops.silver.cancellations                 (cancelled rows, separated first)
                      skyops.silver.quarantine                    (failed reconciliation)
                       ↓  (Lakeflow Job: skyops_gold_refresh → dbt)
                      skyops.gold.dim_date
                      skyops.gold.dim_airport                     (keyed on bts_airport_id)
                      skyops.gold.dim_airline
                      skyops.gold.dim_aircraft                    (keyed on tail_number)
                      skyops.gold.fct_flights                     (dbt incremental, 52.8M rows)
                      skyops.gold.fct_delay_cascade               (cascade pairs via Tail_Number)
                       ↓
                      Power BI (Import mode, SQL Warehouse)
                        └─ dim_origin_airport  (view of dim_airport, active rel on origin_airport_id)
                        └─ dim_dest_airport    (view of dim_airport, active rel on dest_airport_id)
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

9. **ORIGIN IATA code is not downloaded**: The BTS download selected `ORIGIN_CITY_NAME` (city string) and `ORIGIN_AIRPORT_ID` (BTS numeric ID) but not `ORIGIN` (the 3-letter IATA code). `DEST` is available as IATA. **Resolution**: `dim_airport` is keyed on `bts_airport_id` (the numeric), so `fct_flights` carries `origin_airport_id` and `dest_airport_id` as integer FKs. No re-download needed. Do NOT attempt to derive ORIGIN IATA in the fact table — join to `dim_airport` instead.

10. **CRS_DEP_TIME is an unpadded string**: Stored as `"800"` for 08:00, `"2007"` for 20:07. The `flight_id` surrogate key includes `CRS_DEP_TIME`. Normalise to 4-digit padded string with `LPAD(CAST(CAST(CRS_DEP_TIME AS INT) AS STRING), 4, '0')` in `stg_bts_flights.sql` before building the key, or the same flight will have a different key if BTS ever changes padding. The `dep_time_minutes` column (used for cascade join ordering) must also be derived from the padded form.

---

## 11. dbt Models — What Needs To Be Built

### Design: Star Schema (not a flat fact table)
46.8M rows denormalised with airport/airline attributes would be 15–20GB in Power BI Import mode. The star schema keeps `fct_flights` slim with integer FK joins. See ADR-006.

### Grain of `fct_flights`
One row per **scheduled flight leg** — including cancelled flights (flagged, NULL delay metrics, not excluded). Grain enforced by surrogate key.

### Surrogate Key (proven, 0 duplicates on 52.8M rows)
```sql
CONCAT_WS('_',
  FL_DATE,
  AIRLINE_CODE,
  CAST(FL_NUMBER AS STRING),
  LPAD(CAST(CAST(CRS_DEP_TIME AS INT) AS STRING), 4, '0'),  -- normalised to 4 digits
  CAST(OriginAirportID AS STRING),
  CAST(DestAirportID AS STRING),
  COALESCE(Tail_Number, '')  -- NULL → empty string for cancelled flights
) AS flight_id
```
Each column's necessity was validated via EDA in `silver/BTS Primary Key resolution.ipynb`.

### Staging layer (materialized as `view`)
- `stg_bts_flights.sql` — cast types; LPAD CRS_DEP_TIME; compute `dep_time_minutes`, `dep_hour`, `flight_id`; derive boolean flags
- `stg_metar_obs.sql` — **Iteration 2 only** (METAR not yet downloaded)

### Marts layer (materialized as `table` or `incremental`)
- `dim_date.sql` — `dbt_utils.date_spine` from `2019-01-01` to `2026-12-31`; flags: `is_covid_period`, `day_of_week`, `month_name`, `is_weekend`
- `dim_airport.sql` — from `airport_metadata` seed; **primary key is `bts_airport_id`** (numeric), not IATA; includes `iata_code`, `airport_name`, `city`, `state`, `hub_classification`, `lat`, `lon`
- `dim_airline.sql` — from `airline_metadata` seed; keyed on `airline_code` (IATA string)
- `dim_aircraft.sql` — from aircraft dimension table loaded separately; keyed on `tail_number`; includes aircraft type/family
- `fct_flights.sql` — incremental, `unique_key='flight_id'`; FKs: `fl_date`, `origin_airport_id`, `dest_airport_id`, `airline_code`, `tail_number`; pre-computed measures: `is_on_time`, `is_dep_on_time`, `is_cancelled`, `controllable_delay_min`, `uncontrollable_delay_min`, `airborne_recovery_min`, `block_time_padding_min`, `dep_time_minutes`, `dep_hour`
- `fct_delay_cascade.sql` — self-join on `Tail_Number + fl_date`, `ROW_NUMBER() OVER (PARTITION BY fl_date, tail_number ORDER BY dep_time_minutes)`. **NOT** on `airline_code + fl_number` (FL_NUMBER identifies a route-schedule, not a physical aircraft rotation). Excludes NULL Tail_Numbers (0.55% of records, 99.999% cancelled). See ADR-006 and Incident #003 correction.

### Power BI role-playing dimensions
`dim_airport` is imported twice as `dim_origin_airport` and `dim_dest_airport` (two dbt views selecting `* FROM dim_airport`). This gives Power BI two active relationships from `fct_flights` without bidirectional join complexity.

### dbt variables in scope (set in dbt_project.yml)
```yaml
dim_date_start: "2019-01-01"
dim_date_end: "2026-12-31"          # covers full BTS data range
covid_period_start: "2020-03-15"
covid_period_end: "2021-06-01"
delay_reporting_threshold_minutes: 15
delay_reconciliation_tolerance_minutes: 2
incremental_lookback_days: 35
max_freshness_days: 40
```

---

## 12. Suggested Next Steps (Priority Order)

1. **Create `dbt/seeds/` CSV files** (HIGH):
   - `airport_metadata.csv` — must include `bts_airport_id` column (BTS numeric ID, e.g. 10397 for ATL) as the join key to `OriginAirportID`/`DestAirportID` in Bronze
   - `airline_metadata.csv` — keyed on `airline_code` (IATA string matching `AIRLINE_CODE` in Bronze)
   - `airport_timezone.csv` — for Iteration 2 METAR join; can be created now but not used yet

2. **Load aircraft dimension table** to Databricks (source: FAA aircraft registry or similar); must be keyed on `Tail_Number` string.

3. **Build dbt models** in DAG order:
   ```
   dbt seed
   dbt run --select dim_date dim_airline dim_airport dim_aircraft
   dbt run --select stg_bts_flights
   dbt run --select fct_flights
   dbt run --select fct_delay_cascade
   dbt test
   ```

4. **Build Silver notebooks** (`notebooks/silver/silver_flights.py`) — follows 6-step pattern with cancelled-first separation (Incident #004 rule).

5. **Deploy Asset Bundle** to Databricks:
   ```bash
   databricks bundle deploy --target prod
   ```

6. **Connect Power BI** — import `dim_origin_airport` and `dim_dest_airport` as two separate queries pointing to the same `dim_airport` table. Build dashboards for the 5 problem domains.

7. **METAR (Iteration 2)** — build `ingestion/metar_downloader.py`, then `stg_metar_obs.sql`, then enrich `fct_flights` with weather join.