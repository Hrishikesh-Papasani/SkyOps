# Silver Layer - Data Cleansing & Quality

This folder contains **Silver layer** notebooks that transform Bronze raw data into cleaned, validated, and enriched datasets.

## Purpose

The Silver layer applies:
* **Data quality rules**: Null handling, outlier detection, constraint validation
* **Standardization**: Type conversions, format normalization, timezone handling
* **Enrichment**: Joins with reference data (airports, airlines, weather)
* **Deduplication**: Removing duplicate records
* **Slowly Changing Dimensions (SCD)**: Tracking historical changes

## Input/Output

**Input**: Bronze tables from `skyops.bronze.*`  
**Output**: Silver tables to `skyops.silver.*`  
**Transformation**: Medium complexity (cleansing, validation, enrichment)

## Planned Notebooks

### `bts_ontime_clean.py` (Future)
Cleanses and standardizes BTS flight data:
* Parse FL_DATE from string to proper date/timestamp
* Add timezone conversions for departure/arrival times
* Enrich with airport reference data (lat/lon, timezone, region)
* Enrich with airline reference data (full name, alliance)
* Apply data quality rules:
  - Remove flights with invalid airport IDs
  - Flag outliers (e.g., negative delays beyond threshold)
  - Validate date ranges and time sequences
* Create SCD Type 2 tracking for airline/airport changes

**Output table**: `skyops.silver.bts_ontime`

### Other Potential Silver Notebooks
* `weather_data_clean.py` - Weather enrichment
* `airport_reference.py` - Airport dimension table
* `airline_reference.py` - Airline dimension table

## Development Guidelines

1. **Idempotency**: Silver transformations should be rerunnable without side effects
2. **Data Quality Metrics**: Log quality scores (completeness, accuracy, consistency)
3. **Schema Evolution**: Use `.option("mergeSchema", "true")` for additive changes
4. **Incremental Processing**: Process only new/changed data when possible
5. **Testing**: Include data quality validation cells at the end of each notebook

## Naming Conventions

* Notebooks: `<source>_<entity>_clean.py` or `<source>_<entity>_enrich.py`
* Tables: `skyops.silver.<source>_<entity>`
* Example: `bts_ontime_clean.py` → `skyops.silver.bts_ontime`

## Quality Metrics to Track

* Row count change (Bronze → Silver)
* Null rate per column
* Outlier detection rate
* Enrichment success rate (% of rows enriched)
* Data quality score (0-100 scale)

## Next Steps

1. Create `bts_ontime_clean.py` with date/time parsing
2. Add airport reference data pipeline
3. Add airline reference data pipeline
4. Implement data quality dashboard