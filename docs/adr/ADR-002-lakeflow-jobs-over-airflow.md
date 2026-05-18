# ADR-002: Lakeflow Jobs (Databricks Workflows) Over Apache Airflow

**Date:** 2024-01  
**Status:** Accepted  
**Deciders:** Data Engineering

---

## Context

SkyOps requires orchestration of three interdependent batch jobs:
1. `skyops_monthly_ingest` — raw file validation + Bronze landing (triggered by file arrival)
2. `skyops_silver_quality` — business rules + Great Expectations quality gate (triggered by Job 1)
3. `skyops_gold_refresh` — dbt run + tests + freshness check (triggered by Job 2, only if GE passed)

Additionally, a long-running streaming consumer job must operate independently.

Requirements for the orchestrator:
- Event-driven trigger (file arrival on Databricks Volume) for Job 1
- Job-to-job dependency (Job 2 triggers only when Job 1 succeeds; Job 3 triggers only when Job 2 GE task passes)
- Task-level retry with exponential backoff
- Email alerts on failure and SLA breach
- Integration with Unity Catalog lineage (so pipeline runs appear in lineage graph)
- Deployable as code (not click-through UI configuration)

---

## Options Considered

### Option A — Apache Airflow (managed or self-hosted)

**Pros:**
- Industry standard — most interviewers are familiar with it
- Rich operator ecosystem (KubernetesPodOperator, EmrOperator, etc.)
- Strong community, many tutorials

**Cons:**
- Requires a running Airflow instance (either self-hosted in Docker or managed via Astronomer/MWAA)
- Self-hosted: adds ~$15–30/month on cloud VMs, plus Postgres for metadata, plus maintenance
- MWAA (AWS managed Airflow): ~$250/month minimum — unacceptable for a ₹1,500 budget project
- Does not natively integrate with Databricks Unity Catalog lineage
- Airflow DAGs cannot be triggered by file arrival on a Databricks Volume without custom sensor polling
- Additional infra contradicts the cloud-native, no-Docker design principle (ADR-001)

### Option B — Lakeflow Jobs (Databricks Workflows)

**Pros:**
- Zero additional infrastructure — runs inside the Databricks workspace already provisioned
- Native file arrival trigger on Unity Catalog Volumes (no polling sensor needed)
- Job-to-job dependency via `run_job_task` — Job 3 can be conditionally triggered from Job 2
- Cluster-level and task-level retry configuration with exponential backoff
- Email and webhook notifications built in
- All job definitions expressed in `databricks.yml` (Databricks Asset Bundles) — full GitOps
- Run history, Spark UI, and cluster metrics all accessible within same workspace
- Unity Catalog lineage automatically tracks which pipeline run produced which Delta table version

**Cons:**
- Not as widely known as Airflow in general data engineering community
- Cross-cloud or cross-platform orchestration not possible (Airflow's strength)
- DAG visualisation less mature than Airflow's Gantt view

### Option C — AWS Step Functions

**Pros:**
- Serverless, event-driven
- Native integration with S3 triggers

**Cons:**
- Requires Databricks notebooks to be invoked via REST API — complex IAM configuration
- No Spark UI integration
- No Unity Catalog lineage
- Not relevant for Azure-hosted Databricks workspaces

---

## Decision

**Option B — Lakeflow Jobs (Databricks Workflows).**

The decision is primarily driven by the no-infra-overhead design principle established in ADR-001. Lakeflow Jobs provides all required orchestration capabilities from within the existing Databricks workspace at no additional infrastructure cost.

The file-arrival trigger on Unity Catalog Volumes is a specific capability that eliminates the need for scheduled polling sensors — a meaningful production advantage.

Jobs-as-code via `databricks.yml` means the entire pipeline — compute, storage, and orchestration — is version-controlled and reproducible via `databricks bundle deploy`.

For interview purposes: being able to explain why Lakeflow Jobs was chosen over Airflow (and demonstrating awareness of the trade-offs) is more sophisticated than defaulting to Airflow because it is more familiar.

---

## Consequences

**Positive:**
- Zero infra overhead — no Airflow webserver, scheduler, or metadata database to maintain
- File arrival trigger eliminates polling latency for new monthly BTS data
- Full pipeline lineage visible in Databricks Unity Catalog lineage graph
- `databricks bundle deploy` deploys all three jobs atomically — no manual UI clicks

**Negative:**
- Databricks Workflows is a proprietary orchestrator — knowledge does not transfer directly to non-Databricks environments
- Cross-system dependencies (e.g., triggering an external Spark cluster) are not natively supported
- Workflow DAG visualisation is less feature-rich than Airflow's UI for complex pipelines

**Mitigation:**
- The core orchestration concepts (dependency management, retry policies, alerting) transfer across tools
- `databricks.yml` is human-readable YAML — the job dependency graph is clear from the config file itself

---

## Interview Note

When asked "why not Airflow?": Lead with the no-infra argument. Then add: file arrival triggers require either MWAA ($250/month) or a custom polling sensor on self-hosted Airflow. Lakeflow Jobs provides this natively at zero additional cost. The trade-off is Databricks lock-in — which is acceptable given the entire platform already runs on Databricks.

---

## References

- [Databricks Workflows documentation](https://docs.databricks.com/workflows/index.html)
- [Databricks Asset Bundles — jobs configuration](https://docs.databricks.com/dev-tools/bundles/reference.html)
- [Unity Catalog file arrival triggers](https://docs.databricks.com/workflows/jobs/file-arrival-triggers.html)
