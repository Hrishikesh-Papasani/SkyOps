# ADR-006: Star Schema Over a Single Flat Fact Table

**Date:** 2026-05  
**Status:** Accepted  
**Deciders:** Data Engineering

---

## Context

The Gold layer must serve five analytical problem domains (Operations, Route Planning, Airport Operations, Fleet & Turnaround, Schedule Integrity) to a Power BI Import mode dashboard. At 52.8 million rows, the physical layout of the Gold tables directly affects Power BI memory consumption, DAX query speed, and model maintainability.

Two structural approaches were evaluated:

1. **Single flat fact table** — all flight, airline, and airport attributes denormalised into one wide table per flight row
2. **Star schema** — a lean fact table with integer foreign keys, joined to separate dimension tables

---

## Options Considered

### Option A — Single Flat Fact Table

**Approach:** One table, one row per flight, containing all BTS fields plus all dimensional attributes (airport name, city, state, hub class, airline name, LCC flag, alliance, aircraft family, etc.) repeated for every flight.

**Pros:**
- Simplest SQL — no joins at query time
- Power BI model requires only one table — no relationship configuration
- No risk of fan-out or ambiguous join paths in Power BI

**Cons:**
- Airport name, city, state, hub classification repeated twice per row (origin + destination) × 52.8M rows → estimated 15–20GB uncompressed in memory
- Power BI Import mode loads the full uncompressed column store into RAM — exceeds practical laptop limits for a portfolio demo
- Any change to an airport attribute (e.g. hub reclassification) requires reprocessing 52.8M rows in the fact table
- Cannot handle origin/destination role-playing cleanly — storing two copies of airport attributes with prefixed column names (`origin_airport_name`, `dest_airport_name`) doubles the column count and makes DAX measures verbose
- Route-level analysis requires grouping on two string columns (`origin_city + dest_city`) rather than two integer FKs — slower and error-prone (city name formatting inconsistencies)

### Option B — Star Schema

**Approach:** A slim `fct_flights` table carrying only integer foreign keys and pre-computed numeric measures, joined to `dim_airport`, `dim_airline`, `dim_date`, and `dim_aircraft` dimension tables.

**Pros:**
- `fct_flights` carries integer FK columns (`origin_airport_id`, `dest_airport_id`, `airline_code`, `tail_number`, `fl_date`) — dramatically reduces in-memory size vs string columns
- Dimensional attributes are stored once in small tables (dim_airport: ~500 rows, dim_airline: ~20 rows, dim_date: ~2,800 rows) — negligible memory overhead
- Power BI's Vertipaq engine is optimised for integer FK joins — visuals filter faster than on repeated strings
- Airport dimension attribute changes (hub reclassification, name correction) require updating one row in `dim_airport` — zero reprocessing of 52.8M fact rows
- Role-playing dimensions (origin vs destination airport) handled cleanly: `dim_airport` imported twice into Power BI as `dim_origin_airport` and `dim_dest_airport` — two active relationships, standard Kimball pattern

**Cons:**
- Power BI model requires relationship configuration — more setup than a single table
- dbt DAG has more models — staging → dimensions → facts — more surface area for misconfiguration
- `fct_delay_cascade` is a second fact table that creates two FK paths back to `fct_flights` — requires inactive relationship management with `USERELATIONSHIP()` in DAX measures

---

## Decision

**Option B — Star schema.**

The memory argument is decisive for Power BI Import mode. 52.8 million rows with denormalised string attributes will not fit comfortably in 8–16GB RAM on a typical development laptop. The star schema reduces the effective in-memory fact table size by 60–70% compared to the flat alternative, making offline demo presentation viable.

The Kimball dimensional modelling approach also produces a more correct analytical model: the fact table grain (one row per scheduled flight leg) is cleanly separated from slowly-changing dimensional context. This is the industry-standard pattern for airline operational analytics and demonstrates senior-level data modelling judgement.

---

## Key Design Decisions Within the Star Schema

### `dim_airport` — keyed on `bts_airport_id`, not IATA

The BTS Bronze table carries `OriginAirportID` and `DestAirportID` as BTS numeric IDs (e.g. `10397` for ATL). The `DEST` column is a 3-letter IATA code, but no `ORIGIN` IATA column was downloaded. Rather than re-downloading data or deriving ORIGIN IATA from city strings, `dim_airport` uses `bts_airport_id` as its primary key. `fct_flights` foreign keys (`origin_airport_id`, `dest_airport_id`) join to `bts_airport_id` directly. IATA codes are attributes within the dimension, not join keys.

### `fct_delay_cascade` — keyed on `tail_number + fl_date`, not `airline_code + fl_number`

The cascade self-join identifies consecutive legs operated by the same physical aircraft. `FL_NUMBER` identifies a route-schedule (e.g. AA 100 is always JFK→LAX) — not an aircraft rotation. Using `FL_NUMBER` as the cascade join key would find nothing meaningful (an airline does not fly the same route twice on the same day under the same flight number). The correct join key is `Tail_Number + fl_date`, partitioned by `fl_date + tail_number` and ordered by `dep_time_minutes`. This produces a `ROW_NUMBER()` sequence of 4–8 legs per aircraft per day, which is then self-joined on `leg_seq + 1`. NULL `Tail_Number` rows (0.55% of records, 99.999% cancelled) are excluded. See Incident #003 for the performance optimisation that makes this self-join viable at scale.

### Role-playing airport dimensions in Power BI

`dim_airport` is imported into Power BI twice:
- As `dim_origin_airport` — active relationship on `fct_flights[origin_airport_id]`
- As `dim_dest_airport` — active relationship on `fct_flights[dest_airport_id]`

Both are implemented as dbt views (`SELECT * FROM {{ ref('dim_airport') }}`), so they carry zero additional storage cost in Databricks. This allows Power BI slicers on origin city and destination city simultaneously without bidirectional relationship complexity.

### Cancelled flights remain in `fct_flights`

Cancelled flights (`is_cancelled = TRUE`) are included in `fct_flights` with NULL delay metrics, not excluded to a separate Gold table. This allows a single Power BI visual to show on-time rate, cancellation rate, and diversion rate from one table with simple `is_cancelled` / `is_diverted` flag filters. The Silver layer still separates cancelled rows for quality validation (Incident #004), but the Gold layer re-unions them with the flag set.

---

## Consequences

**Positive:**
- Power BI Import mode memory footprint: ~1.5–2.5GB for full Gold star schema (vs 15–20GB flat)
- Dashboard slicers and cross-filtering respond in < 1 second on a standard laptop (ADR-004 Import mode requirement)
- Airport and airline dimension changes require no fact table reprocessing
- Analytical queries on the Databricks SQL Warehouse are partition-pruned on `fl_date` and cluster-pruned on `airline_code + origin_airport_id`

**Negative:**
- Power BI model requires 6 relationships to be configured correctly (fct_flights → 4 dims + fct_delay_cascade → fct_flights × 2 inactive)
- `fct_delay_cascade` DAX measures must use `USERELATIONSHIP()` to activate the upstream/downstream flight relationships — slightly more complex DAX than a flat model
- `dim_aircraft` requires a separately sourced and loaded aircraft registry (FAA registry or equivalent) — not available in BTS data; must be loaded before `fct_flights` can be validated against it
