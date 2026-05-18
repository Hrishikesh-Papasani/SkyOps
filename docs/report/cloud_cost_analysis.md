# SkyOps Cloud Cost Analysis

**Period:** 10-week build (Phase 1–5)  
**Platform:** Databricks (AWS ap-south-1 / Azure Central India) + Confluent Cloud  
**Workspace:** _fill in workspace URL after setup_  
**Report Author:** Data Engineering  
**Last Updated:** _fill in after project completion_

---

> **This document records actual spend from the Databricks billing UI.**  
> Estimates are provided as planning references. Replace with actual figures after each week.  
> Screenshot the final billing page and add to `docs/report/charts/billing_screenshot.png`.

---

## Cost Principles

1. **Serverless compute only** — no always-on clusters. Pay per second of notebook execution.
2. **SQL Warehouse auto-stop** — configured at 10 minutes idle. Not left running between sessions.
3. **Development data** — 2022 data only (~6M rows) during Phases 1–4. Full 29M row load only in Phase 4 weekend run.
4. **Budget alert** — $30/month alert set in Databricks billing settings on Day 1.
5. **Confluent trial** — $400 credit covers all streaming work. Cluster paused when not in active development.

---

## Weekly Spend Log

_Fill in after each week from Databricks billing UI (Workspace → Admin → Cost Management)._

| Week | Phase | Activity | DBU Estimate | Actual DBU | Actual USD |
|------|-------|----------|-------------|------------|------------|
| 1 | Setup | Workspace config, Unity Catalog setup, 2022 data upload, notebook exploration | ~5 DBU | _TBD_ | _TBD_ |
| 2 | Setup | Confluent setup, secret scope, initial Bronze exploration | ~3 DBU | _TBD_ | _TBD_ |
| 3 | Bronze/Silver | bronze_bts.py + bronze_metar.py development runs | ~8 DBU | _TBD_ | _TBD_ |
| 4 | Bronze/Silver | silver_flights.py (6 steps), METAR join, quarantine testing | ~12 DBU | _TBD_ | _TBD_ |
| 5 | Gold/Streaming | dbt setup + all model runs, dbt tests | ~10 DBU SQL Warehouse | _TBD_ | _TBD_ |
| 6 | Gold/Streaming | Kafka producer + streaming consumer development | ~8 DBU | _TBD_ | _TBD_ |
| 7 | Orchestration | Lakeflow Jobs configuration, failure path testing | ~15 DBU | _TBD_ | _TBD_ |
| 8 | Full Load | Full 29M row load (weekend run), 5-year pipeline end-to-end | ~40 DBU | _TBD_ | _TBD_ |
| 9 | Power BI | Power BI connection, dashboard build, refresh testing | ~5 DBU SQL Warehouse | _TBD_ | _TBD_ |
| 10 | Docs | Documentation, demo video, final dbt docs generate | ~3 DBU | _TBD_ | _TBD_ |
| **TOTAL** | | | **~109 DBU est.** | **_TBD_** | **_TBD_** |

---

## Cost Breakdown by Component

### Databricks Serverless Compute (Notebooks)

| Activity | Estimated DBU | Estimated Cost (USD) |
|----------|--------------|----------------------|
| Bronze jobs (development + test runs) | 20 DBU | $4–6 |
| Silver jobs (development + test runs) | 25 DBU | $5–8 |
| Streaming consumer (always-on during dev) | 15 DBU | $3–5 |
| Lakeflow Jobs testing (all 3 jobs) | 20 DBU | $4–6 |
| Full 29M row pipeline run (Phase 4) | 40 DBU | $8–12 |
| **Subtotal** | **~120 DBU** | **$24–37** |

_Serverless notebook DBU rate: ~$0.20–0.25/DBU (varies by region)_

### Databricks SQL Warehouse (dbt + Power BI)

| Activity | Estimated DBU | Estimated Cost (USD) |
|----------|--------------|----------------------|
| dbt model development runs (2X-Small) | 15 DBU | $3–5 |
| dbt full run on 29M rows (Phase 4) | 10 DBU | $2–3 |
| Power BI Import refresh (5 refreshes) | 5 DBU | $1–2 |
| Databricks SQL Dashboard queries | 3 DBU | $0.50–1 |
| **Subtotal** | **~33 DBU** | **$6.50–11** |

_SQL Warehouse serverless DBU rate: ~$0.22/DBU (2X-Small)_

### Databricks Unity Catalog Storage (Delta tables)

| Data | Estimated Size | Estimated Cost/Month |
|------|---------------|----------------------|
| Bronze flights (29M rows, Delta) | ~25GB | ~$0.50 |
| Bronze weather (15 airports × 5 years) | ~5GB | ~$0.10 |
| Silver flights + cancellations + quarantine | ~30GB | ~$0.60 |
| Gold tables (fact + 5 dims) | ~15GB | ~$0.30 |
| Streaming agg + pipeline_status | ~1GB | ~$0.02 |
| **Total storage** | **~76GB** | **~$1.52/month** |

_Delta storage: ~$0.023/GB/month on AWS S3 (ap-south-1)_  
_10-week total storage cost: ~$3.50_

### Confluent Cloud

| Component | Cost |
|-----------|------|
| Basic cluster (within $400 trial) | $0 |
| Schema Registry (within trial) | $0 |
| **Subtotal** | **$0** |

_$400 trial credit expires 30 days after activation. All streaming development planned within this window._

### Power BI Desktop

| Component | Cost |
|-----------|------|
| Power BI Desktop (Windows app) | $0 — free download |
| Publishing `.pbix` to GitHub | $0 |
| **Subtotal** | **$0** |

---

## Total Estimated vs Actual

| Category | Estimated (USD) | Actual (USD) |
|----------|----------------|--------------|
| Databricks Serverless Compute | $24–37 | _TBD_ |
| Databricks SQL Warehouse | $6.50–11 | _TBD_ |
| Databricks Storage | $3–5 | _TBD_ |
| Confluent Cloud | $0 | _TBD_ |
| Power BI Desktop | $0 | _TBD_ |
| **TOTAL** | **$33.50–53** | **_TBD_** |

_In INR at 1 USD ≈ ₹83: **₹2,780–4,400 estimated**_  
_Target from project spec: ₹1,500–2,300 — monitor weekly, reduce if tracking above estimate_

---

## Cost Management Actions Taken

_Fill in as each action is taken._

- [ ] Budget alert set at $30/month (Week 1)
- [ ] SQL Warehouse auto-stop configured at 10 minutes (Week 1)
- [ ] Development restricted to 2022 data only through Week 7
- [ ] Confluent cluster paused after each streaming development session
- [ ] Full 29M row load planned for a single weekend run (Week 8) to batch DBU consumption
- [ ] `OPTIMIZE` and `VACUUM` run once after full load (not repeated unnecessarily)

---

## Billing Screenshot

_Add after project completion._

> `docs/report/charts/billing_screenshot.png` — screenshot from Databricks Admin → Cost Management showing total spend breakdown by SKU (Jobs Compute, SQL, Storage).

This screenshot is uniquely credible in a portfolio context. No tutorial project provides real cloud billing data. Interviewers at Flipkart, Amazon India, and PhonePe consistently ask about cloud cost management — having the actual bill is a differentiator.

---

## Cost per Analytical Finding

Total cost / 7 analytical findings = approximately $5–8 per finding.

This framing (cost per business outcome) is a useful interview talking point when asked about ROI of data engineering investment.
