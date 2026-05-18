# Gold Layer - Business-Level Aggregations

This folder contains **Gold layer** notebooks that create business-ready, aggregated datasets and metrics for analytics and reporting.

## Purpose

The Gold layer provides:
* **Business KPIs**: On-time performance, delay metrics, capacity utilization
* **Aggregated facts**: Daily/monthly summaries, route statistics, airline performance
* **Dimensional models**: Star schemas for BI tools (Tableau, Power BI)
* **Feature engineering**: ML-ready datasets for predictive models
* **Optimized for queries**: Pre-aggregated for fast dashboard performance

## Input/Output

**Input**: Silver tables from `skyops.silver.*`  
**Output**: Gold tables to `skyops.gold.*`  
**Transformation**: High-level aggregations and business logic

## Planned Notebooks

### `flight_performance_daily.py` (Future)
Daily flight performance metrics:
* On-time performance rate by airline, airport, route
* Average delays (departure, arrival, by delay type)
* Cancellation and diversion rates
* Flight volume trends

**Output table**: `skyops.gold.flight_performance_daily`

### `route_analytics.py` (Future)
Route-level analytics:
* Busiest routes by volume
* Most delayed routes
* Average load factors by route
* Seasonal trends by route

**Output table**: `skyops.gold.route_analytics`

### `airline_scorecard.py` (Future)
Airline performance scorecard:
* On-time performance rankings
* Customer satisfaction proxies (delay rates, cancellations)
* Operational efficiency metrics
* Year-over-year comparisons

**Output table**: `skyops.gold.airline_scorecard`

### `ml_features.py` (Future)
ML-ready feature store:
* Delay prediction features (weather, time of day, airport congestion)
* Cancellation risk features
* Demand forecasting features
* Feature versioning and lineage

**Output table**: `skyops.gold.ml_delay_features`

## Use Cases

* **Executive Dashboards**: High-level KPIs for leadership
* **Operational Analytics**: Real-time monitoring for operations teams
* **Customer Analytics**: On-time performance reporting for passengers
* **ML Models**: Training data for predictive models
* **Regulatory Reporting**: Compliance reports for FAA/DOT

## Development Guidelines

1. **Pre-aggregation**: Compute expensive aggregations once, cache results
2. **Incremental updates**: Use Delta merge for daily updates, not full rewrites
3. **Partitioning**: Partition by date/month for time-series queries
4. **Indexing**: Use Z-ordering on frequently filtered columns
5. **Documentation**: Document business logic and metric definitions clearly

## Naming Conventions

* Notebooks: `<business_domain>_<metric_type>.py`
* Tables: `skyops.gold.<business_domain>_<granularity>`
* Examples:
  - `flight_performance_daily.py` → `skyops.gold.flight_performance_daily`
  - `airline_scorecard.py` → `skyops.gold.airline_scorecard`

## Performance Optimization

* **Z-Ordering**: Apply on high-cardinality columns (airline, airport)
* **Liquid Clustering**: Enable for frequently filtered dimensions
* **Predictive Optimization**: Let Databricks auto-tune storage layout
* **Materialized Views**: Consider for complex, frequently-run aggregations

## Quality Metrics

* Query performance (p50, p95, p99 latency)
* Data freshness (time since last update)
* Aggregation accuracy (spot-check against Silver layer)
* Coverage (% of Silver data represented in Gold)

## Next Steps

1. Create `flight_performance_daily.py` for basic KPIs
2. Set up automated refresh schedule (daily at 6 AM)
3. Create BI dashboard connecting to Gold tables
4. Implement ML feature pipeline for delay prediction