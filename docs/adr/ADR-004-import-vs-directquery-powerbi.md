# ADR-004: Import Mode Over DirectQuery for Power BI

**Date:** 2024-04  
**Status:** Accepted  
**Deciders:** Data Engineering

---

## Context

Power BI Desktop connects to Databricks SQL Warehouse to serve the SkyOps flight operations dashboard (4 pages + pipeline health). Two connection modes are available in the Databricks Power BI connector:

1. **Import mode** — Power BI downloads data from Databricks into an in-memory columnar store on the local machine. Queries run against this local cache.
2. **DirectQuery mode** — Power BI sends queries to Databricks SQL Warehouse in real time on each visual interaction. No local cache.

The choice affects: dashboard interactivity speed, Databricks SQL Warehouse cost, data freshness granularity, and complexity of the refresh workflow.

---

## Options Considered

### Option A — DirectQuery

**Pros:**
- Always shows latest data — no refresh required
- No local storage constraints (handles very large Gold tables)
- Power BI slicers generate SQL queries in real time — no pre-aggregation needed

**Cons:**
- Every slicer change, visual interaction, and page load sends queries to Databricks SQL Warehouse
  - A 4-page dashboard with 6 visuals per page = potentially 24 SQL queries per page navigation
  - SQL Warehouse must be running at all times Power BI is open → idle cost accumulates
  - 2X-Small serverless SQL Warehouse: ~$0.22/DBU. Continuous operation during a demo: ~$0.50/hour
- Response time depends on network latency + SQL Warehouse query planning → 2–8 seconds per visual
- Slicer interactions feel sluggish — unacceptable for a live interview demo
- Some Power BI DAX measures behave differently in DirectQuery mode (aggregation pushdown limitations)
- SQL Warehouse auto-stop means first interaction after idle period has 20–30 second warm-up delay

### Option B — Import Mode

**Pros:**
- All 4 dashboard pages load in < 1 second after import (Gold layer fits in ~800MB compressed in-memory)
- No SQL Warehouse required during demo — can present offline
- Slicer interactions, cross-filtering, and drill-through are instantaneous
- Power BI's in-memory Vertipaq engine is highly optimised for columnar aggregations
- Refresh is explicit and controlled — triggered after `skyops_gold_refresh` completes (Power BI reads `pipeline_status` table to display last refresh timestamp)
- Consistent demo experience regardless of network conditions or cloud region latency

**Cons:**
- Data is stale between refreshes — dashboard shows data as of last Gold pipeline run (acceptable for monthly BTS data)
- Local machine RAM usage during refresh: ~1.5–2.5GB for 52.8M rows at Gold star schema level
  - Mitigated by connecting to pre-aggregated Gold tables (star schema slim fact + small dims), not Silver raw rows
  - `dim_airport` is imported twice as `dim_origin_airport` and `dim_dest_airport` (role-playing pattern) — adds negligible size since dim_airport is ~500 rows
- Refresh requires SQL Warehouse to be running — but only for the duration of the refresh (~3–5 minutes), not continuously

### Option C — Composite Model (Import + DirectQuery mixed)

**Pros:**
- Import frequently-used aggregated tables; DirectQuery real-time tables (streaming_agg)

**Cons:**
- Significantly more complex to configure
- Requires Power BI Premium or Power BI Service for some composite model features
- Overkill for a portfolio project — adds complexity without demonstrating additional skill

---

## Decision

**Option B — Import mode.**

Primary driver: **interview demo reliability**. A dashboard that responds in < 1 second to slicer changes is demonstrably better than one that takes 3–8 seconds per interaction, regardless of which mode is "more real-time."

Secondary driver: **cost**. Import mode requires SQL Warehouse only during the explicit refresh step (~3–5 minutes), not during the entire presentation session. This aligns with the cost-minimisation principles of the project.

The monthly BTS data cadence means the dashboard is refreshed at most once per month — Import mode's "stale between refreshes" limitation is structurally irrelevant for this dataset.

---

## Consequences

**Positive:**
- Dashboard loads instantly in demo — no awkward wait times
- Can present from any location without active Databricks connection (offline demo possible)
- Power BI Desktop is free — no Power BI Pro licence required for building and sharing `.pbix` file
- Refresh workflow is explicit: Job 3 writes to `pipeline_status` → analyst opens Power BI → clicks Refresh → 3–5 minutes → dashboard updated

**Negative:**
- `.pbix` file size increases with imported data (~800MB for full Gold layer) — Git LFS required for version control
- Dashboard must be manually refreshed after each pipeline run — or via Power BI Service scheduled refresh (requires Pro licence, not used here)
- Import does not reflect mid-month BTS corrections until next explicit refresh

**Refresh Trigger Workflow:**
```
skyops_gold_refresh completes
→ pipeline_status_update task writes completion record to skyops.gold.pipeline_status
→ Power BI last-refresh card on Pipeline Health page shows this timestamp
→ Analyst opens Power BI Desktop → Home → Refresh
→ Import pulls from skyops.gold.fact_flights + all dims (~3–5 min)
→ Dashboard updated
```

---

## Streaming Aggregations Exception

The `skyops.gold.streaming_agg` table (15-minute tumbling window results from Confluent Cloud) is **not** included in the main Power BI Import dataset. It is shown separately via a Databricks SQL Dashboard (native to the Databricks workspace) for the streaming demonstration, where DirectQuery to streaming_agg is appropriate.

---

## References

- [Power BI DirectQuery in Databricks](https://docs.databricks.com/partners/bi/power-bi.html)
- [Power BI Import vs DirectQuery guidance](https://docs.microsoft.com/en-us/power-bi/connect-data/desktop-directquery-about)
- [Vertipaq engine compression rationale](https://www.sqlbi.com/articles/understanding-the-vertipaq-engine/)
