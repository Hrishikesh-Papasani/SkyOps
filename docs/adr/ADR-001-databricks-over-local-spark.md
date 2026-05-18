# ADR-001: Databricks Serverless Over Local Spark + Docker

**Date:** 2024-01  
**Status:** Accepted  
**Deciders:** Data Engineering

---

## Context

SkyOps processes ~29 million BTS flight records across 5 years, joins with NOAA METAR weather data at the hourly level, and serves a Power BI dashboard from a Star Schema gold layer. The pipeline requires:

- Managed Spark execution for PySpark transformations at scale
- Delta Lake with ACID transactions and time travel
- Unity Catalog for column-level lineage and access control
- A single, coherent data platform that a solo engineer can operate without DevOps overhead

The original stack considered was: Apache Spark running locally in Docker, with MinIO as an S3-compatible object store, and open-source Delta Lake.

---

## Options Considered

### Option A — Local Spark in Docker + MinIO

**Pros:**
- Zero cloud cost
- Full local reproducibility
- Familiar to candidates who have used tutorial stacks

**Cons:**
- Requires 32GB+ RAM machine to run meaningfully on 29M rows
- MinIO setup and maintenance is significant non-pipeline work
- Unity Catalog is not available outside Databricks — no lineage, no column-level access
- Cannot demonstrate cloud cost management (a differentiator in senior interviews)
- Docker dependency chain introduces fragility (version conflicts, `docker-compose up` failures in demos)
- Power BI cannot connect to local Spark directly — requires additional serving layer

### Option B — Databricks Serverless (Paid Workspace)

**Pros:**
- Managed Spark — no cluster configuration required for serverless notebooks
- Unity Catalog included — column lineage, schema enforcement, volume management
- Databricks Volumes replace MinIO for raw file landing (integrated, no external config)
- Serverless compute charges per-second — zero idle cost
- Direct Power BI connector to Databricks SQL Warehouse
- Databricks Asset Bundles provide jobs-as-code CI/CD (analogous to Terraform for pipelines)
- Demonstrates real cloud cost management — actual billing UI screenshots are uniquely credible

**Cons:**
- Costs money (~$23–40 USD over 10 weeks)
- Requires internet access during development

### Option C — AWS EMR / Google Dataproc

**Pros:**
- Cloud-managed Spark
- Cheaper per CPU-hour than Databricks

**Cons:**
- No Unity Catalog — would need Glue Data Catalog (AWS) or equivalent
- No direct Power BI connector
- No built-in Lakeflow Jobs equivalent — requires separate Airflow or Step Functions
- Harder to set up dbt integration
- More configuration work for a portfolio project, less differentiated for Indian product company interviews

---

## Decision

**Option B — Databricks Serverless (Paid Workspace).**

The ₹1,500–2,300 investment over 10 weeks is justified by:
1. Unity Catalog lineage is a specific interview topic at Flipkart and Amazon India data engineering rounds
2. Serverless compute eliminates the largest operational risk in portfolio projects: "works on my machine"
3. The real billing screenshot in `docs/report/cloud_cost_analysis.md` demonstrates cost-conscious production thinking — no other portfolio project provides this
4. Power BI connects directly — no intermediate serving layer required

---

## Consequences

**Positive:**
- All compute, storage, and orchestration managed by Databricks — zero infra maintenance
- Unity Catalog enforces schema and tracks lineage automatically
- Databricks Asset Bundles (`databricks.yml`) provide reproducible deployment

**Negative:**
- Requires active Databricks subscription — project cannot be run entirely offline
- Must configure budget alerts in Week 1 to avoid unexpected charges (see `docs/report/cost_management.md`)
- Serverless cluster cold start adds ~30 seconds to notebook execution — acceptable for batch

**Mitigation:**
- Budget alert set at $30/month in Databricks billing settings
- Auto-stop on SQL Warehouse after 10 minutes idle
- Development uses 2022 data only (~6M rows) until Phase 4 full load

---

## References

- [Databricks Unity Catalog documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Databricks Serverless compute overview](https://docs.databricks.com/clusters/serverless.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
