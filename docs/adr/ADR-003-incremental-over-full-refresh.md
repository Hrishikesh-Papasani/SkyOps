# ADR-003: Incremental Model Over Full Refresh for fact_flights

**Date:** 2024-02  
**Status:** Accepted  
**Deciders:** Data Engineering

---

## Context

`fact_flights` is the central Gold table. It contains ~29 million rows when fully loaded (2019–2023) and grows by ~500,000 rows per month as new BTS data arrives. It is refreshed by the `skyops_gold_refresh` Lakeflow job after each Silver pipeline run.

Two materialisation strategies were evaluated for the dbt `fact_flights` model:
1. **Full refresh** — truncate and reload the entire table on every pipeline run
2. **Incremental (merge)** — process only new or recently-arrived records, merging on `flight_id`

The choice affects: runtime, cost, historical data preservation, and handling of late-arriving records.

---

## Options Considered

### Option A — Full Refresh

**Pros:**
- Simple — no incremental state to manage
- Idempotent by definition — re-running always produces correct output
- No late-arriving record edge cases
- Easier to reason about correctness

**Cons:**
- Processing 29M rows on every run is expensive and slow on Databricks SQL Warehouse
  - Estimated full refresh time: 25–35 minutes on 2X-Small serverless warehouse
  - Cost per full refresh: ~$0.80 on SQL Warehouse at serverless pricing
  - Monthly cost for 4 runs: ~$3.20 (acceptable but unnecessary)
- Cannot use `OPTIMIZE` and `ZORDER` incrementally — full table rewrite on each run
- Overwrites historical data — no Delta time travel benefit between runs
- All 29M rows scanned every run, even when only 500K rows changed

### Option B — Incremental (Merge on flight_id)

**Pros:**
- Processes only new and recently-modified records — runtime scales with monthly delta, not historical total
- 35-day lookback window handles late-arriving BTS records (BTS sometimes corrects data retroactively up to 30 days)
- Delta MERGE is ACID — no partial writes
- `OPTIMIZE` and `ZORDER` run incrementally on new partitions only
- Estimated incremental run time for 1 month of new data: 4–8 minutes

**Cons:**
- Incremental state must be managed correctly — `is_incremental()` macro must be applied consistently
- Requires `unique_key` to be globally unique — `flight_id` (surrogate key from 6 natural key columns) must be collision-free
- Late-arriving records beyond 35 days require a full refresh (rare, but documented)
- More complex to test — need to verify merge behaviour with duplicate `flight_id` in source

---

## Decision

**Option B — Incremental model with merge strategy and 35-day lookback.**

Configuration:
```sql
{{ config(
    materialized='incremental',
    unique_key='flight_id',
    incremental_strategy='merge',
    partition_by={
        'field': 'fl_date',
        'data_type': 'date',
        'granularity': 'month'
    },
    cluster_by=['airline_code', 'origin_iata']
) }}
```

The 35-day lookback window:
```sql
{% if is_incremental() %}
WHERE to_date(FL_DATE, 'yyyyMMdd') >=
      dateadd(DAY, -35, (SELECT max(fl_date) FROM {{ this }}))
{% endif %}
```

**Rationale for 35 days (not 30):** BTS data corrections have been observed up to 30 days after initial publication. 5-day buffer provides safety margin without scanning significantly more data.

**Surrogate key collision check:** `flight_id = md5(FL_DATE || AIRLINE_CODE || FL_NUMBER || ORIGIN || DEST || CRS_DEP_TIME)`. The combination of these six fields is unique per BTS data dictionary — a flight cannot have the same airline, flight number, route, and scheduled departure time twice on the same date. Verified with `dbt test unique: flight_id` on full 2022 dataset (0 violations).

---

## Consequences

**Positive:**
- Monthly pipeline runtime: 4–8 minutes vs 25–35 minutes for full refresh
- Delta MERGE ensures ACID correctness — no duplicate rows even on re-runs
- Partition pruning on `fl_date` (month granularity) + clustering on `airline_code` / `origin_iata` enables Power BI queries to scan only relevant partitions
- `OPTIMIZE ZORDER BY (airline_code, origin_iata)` runs in ~90 seconds on a monthly partition vs 15+ minutes on the full table

**Negative:**
- If `flight_id` surrogate key generation logic changes, a full refresh is required
- Late-arriving BTS corrections beyond 35 days require manual `dbt run --full-refresh` — must be documented in runbook
- Incremental state (`max(fl_date)` in target table) must be non-null before first incremental run — `dbt run` handles this automatically on first execution (falls through to full load)

**Edge Case — Full Refresh Trigger:**
```bash
# Trigger a full refresh if late-arriving data beyond 35-day window is detected
dbt run --select fact_flights --full-refresh
```
Document in runbook when this is required. Expected frequency: 0–1 times per year.

---

## Data Freshness SLA

The `data_freshness_check` task in Job 3 asserts `max(fl_date)` in `fact_flights` is within 40 days of current date. This SLA covers:
- 1 month of BTS data publication lag
- 5-day pipeline processing buffer
- 5-day alerting window before breach

---

## References

- [dbt incremental models documentation](https://docs.getdbt.com/docs/build/incremental-models)
- [Delta Lake MERGE documentation](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)
- BTS data correction policy — corrections published within 30 days of initial month release
