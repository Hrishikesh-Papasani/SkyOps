# SkyOps — New Machine Setup

## Prerequisites
- Python 3.10+ installed
- Git installed
- Access to the GitHub/Azure DevOps repo

## 1. Clone the repo
```bash
git clone <your-repo-url> SkyOps
cd SkyOps
```

## 2. Create virtual environment and install dependencies
```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Mac/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

## 3. Set environment variables (Databricks credentials)
Create a `.env` file in the repo root (it is gitignored):
```
DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
DBT_TOKEN=<your-databricks-pat>
```
Load it before running dbt:
```bash
# Windows PowerShell
Get-Content .env | ForEach-Object { $name,$value = $_ -split '=',2; Set-Item "env:$name" $value }

# Mac/Linux
export $(cat .env | xargs)
```

## 4. Test the BTS downloader (single month)
```bash
python ingestion/bts_downloader.py --month 2024-01 --output-dir ./data/raw/bts --no-combine --verbose
```
Expected: ~21 MB ZIP downloaded, `data/raw/bts/staging/2024_01.csv` created with 547,271 rows × 32 columns.

## 5. Run the full 4-year download
```bash
python ingestion/bts_downloader.py --years 4 --output-dir ./data/raw/bts --output-format parquet
```
- Takes ~3-4 hours (3-second polite delay between each monthly request)
- Resume-safe: re-running skips already-downloaded months automatically
- Output: `data/raw/bts/bts_ontime_YYYY_MM_YYYY_MM.parquet`

## 6. Upload data to Databricks
```bash
# Install Databricks CLI if not installed
pip install databricks-cli

# Upload to Unity Catalog Volumes
databricks fs cp data/raw/bts/bts_ontime_*.parquet \
    dbfs:/Volumes/skyops/raw/bts/ --recursive
```

## 7. Deploy Databricks Asset Bundle
```bash
databricks bundle deploy --target prod
```

## 8. Run dbt models (after Databricks Bronze layer is populated)
```bash
cd dbt
dbt deps                 # install packages
dbt seed                 # load airport/airline/timezone seed data
dbt run --target prod    # build Silver + Gold models
dbt test --target prod   # run data quality tests
```
