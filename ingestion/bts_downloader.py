#!/usr/bin/env python3
"""
bts_downloader.py — BTS On-Time Performance Direct Downloader
==============================================================

Downloads BTS Reporting Carrier On-Time Performance data directly from
the BTS TranStats portal:
  https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&...

No Selenium or browser required. Uses requests + BeautifulSoup to:
  1. Detect the latest available month from the page header dynamically.
  2. Calculate the start date (N years back from latest available).
  3. For each month: submit the form with the selected field checkboxes,
     download the ZIP, extract the CSV, rename columns to our schema.
  4. Save per-month staging CSVs, then concatenate into a single output file.

FORM INTERACTION NOTE (ASP.NET WebForms):
  The BTS page uses ASP.NET WebForms state (__VIEWSTATE, __EVENTVALIDATION).
  Changing the year dropdown requires a PostBack (sets __EVENTTARGET=cboYear)
  to get a fresh ViewState bound to that year. Without this, the download
  POST returns an HTML error page instead of a ZIP file.
  This script handles the PostBack automatically on each year boundary.

COLUMN MAPPING:
  Form checkbox name  →  BTS CSV display name  →  Our output column name
  FL_DATE             →  FlightDate            →  FL_DATE
  OP_UNIQUE_CARRIER   →  Reporting_Airline     →  AIRLINE_CODE
  ... (see RENAME_MAP below for full mapping)

USAGE:
  # Download last 4 years (default) to ./data/raw/bts/
  python ingestion/bts_downloader.py

  # Custom years and output directory
  python ingestion/bts_downloader.py --years 4 --output-dir ./data/raw/bts

  # Write to Databricks Volume (run from within Databricks)
  python ingestion/bts_downloader.py --output-dir /Volumes/skyops/raw/bts

  # Single month (useful for backfill or retry)
  python ingestion/bts_downloader.py --month 2023-06

  # Re-download even if staging file exists
  python ingestion/bts_downloader.py --force

  # Keep monthly files only, skip combining into single file
  python ingestion/bts_downloader.py --no-combine

  # Output CSV instead of Parquet (not recommended for large datasets)
  python ingestion/bts_downloader.py --output-format csv

REQUIREMENTS:
  pip install requests beautifulsoup4 pandas pyarrow python-dateutil lxml

ESTIMATED RUNTIME:
  ~3-4 hours for 4 years (49 months) with 3-second polite delay per request.
  Resume is automatic: already-downloaded staging CSVs are skipped.
"""

import argparse
import io
import logging
import os
import re
import sys
import time
import zipfile
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

BTS_URL = (
    "https://www.transtats.bts.gov/"
    "DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=N8vn6v10%20f722146%20gnoyr5"
)
# Form action uses '+' for spaces (ASP.NET URL encoding)
FORM_POST_URL = (
    "https://www.transtats.bts.gov/"
    "DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=N8vn6v10+f722146+gnoyr5"
)

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ── Form checkbox names (internal BTS DB column names) ───────────────────────
# These are the `name` attributes of the <input type="checkbox"> elements
# on the BTS TranStats form. They differ from the CSV column headers that
# BTS uses in the downloaded file (see RENAME_MAP below).
FORM_CHECKBOXES: List[str] = [
    "FL_DATE",               # → FlightDate          → FL_DATE
    "OP_UNIQUE_CARRIER",     # → Reporting_Airline   → AIRLINE_CODE
    "OP_CARRIER_AIRLINE_ID", # → DOT_ID_Reporting_Airline → DOT_CODE
    "OP_CARRIER_FL_NUM",     # → Flight_Number_Reporting_Airline → FL_NUMBER
    "ORIGIN_AIRPORT_ID",     # → OriginAirportID     → OriginAirportID
    "DEST_AIRPORT_ID",       # → DestAirportID       → DestAirportID
    "ORIGIN_CITY_NAME",      # → OriginCityName      → ORIGIN_CITY
    "DEST",                  # → Dest                → DEST
    "DEST_CITY_NAME",        # → DestCityName        → DEST_CITY
    "CRS_DEP_TIME",          # → CRSDepTime          → CRS_DEP_TIME
    "DEP_TIME",              # → DepTime             → DEP_TIME
    "DEP_DELAY",             # → DepDelay            → DEP_DELAY
    "TAXI_OUT",              # → TaxiOut             → TAXI_OUT
    "WHEELS_OFF",            # → WheelsOff           → WHEELS_OFF
    "WHEELS_ON",             # → WheelsOn            → WHEELS_ON
    "TAXI_IN",               # → TaxiIn              → TAXI_IN
    "CRS_ARR_TIME",          # → CRSArrTime          → CRS_ARR_TIME
    "ARR_TIME",              # → ArrTime             → ARR_TIME
    "ARR_DELAY",             # → ArrDelay            → ARR_DELAY
    "CANCELLED",             # → Cancelled           → CANCELLED
    "CANCELLATION_CODE",     # → CancellationCode    → CANCELLATION_CODE
    "DIVERTED",              # → Diverted            → DIVERTED
    "CRS_ELAPSED_TIME",      # → CRSElapsedTime      → CRS_ELAPSED_TIME
    "ACTUAL_ELAPSED_TIME",   # → ActualElapsedTime   → ELAPSED_TIME
    "AIR_TIME",              # → AirTime             → AIR_TIME
    "DISTANCE",              # → Distance            → DISTANCE
    "CARRIER_DELAY",         # → CarrierDelay        → DELAY_DUE_CARRIER
    "WEATHER_DELAY",         # → WeatherDelay        → DELAY_DUE_WEATHER
    "NAS_DELAY",             # → NASDelay            → DELAY_DUE_NAS
    "SECURITY_DELAY",        # → SecurityDelay       → DELAY_DUE_SECURITY
    "LATE_AIRCRAFT_DELAY",   # → LateAircraftDelay   → DELAY_DUE_LATE_AIRCRAFT
    "TAIL_NUM",              # → Tail_Number         → Tail_Number
]

# ── Column rename map ─────────────────────────────────────────────────────────
# Maps BTS CSV display names (Source Headers) → our schema names (Updated Headers).
# Also includes internal DB name fallbacks in case BTS returns raw column names.
RENAME_MAP: Dict[str, str] = {
    # BTS CSV display names (what appears in the downloaded CSV file)
    "FlightDate":                    "FL_DATE",
    "Reporting_Airline":             "AIRLINE_CODE",
    "DOT_ID_Reporting_Airline":      "DOT_CODE",
    "Flight_Number_Reporting_Airline": "FL_NUMBER",
    "OriginAirportID":               "OriginAirportID",     # keep as-is
    "DestAirportID":                 "DestAirportID",       # keep as-is
    "OriginCityName":                "ORIGIN_CITY",
    "Dest":                          "DEST",
    "DestCityName":                  "DEST_CITY",
    "CRSDepTime":                    "CRS_DEP_TIME",
    "DepTime":                       "DEP_TIME",
    "DepDelay":                      "DEP_DELAY",
    "TaxiOut":                       "TAXI_OUT",
    "WheelsOff":                     "WHEELS_OFF",
    "WheelsOn":                      "WHEELS_ON",
    "TaxiIn":                        "TAXI_IN",
    "CRSArrTime":                    "CRS_ARR_TIME",
    "ArrTime":                       "ARR_TIME",
    "ArrDelay":                      "ARR_DELAY",
    "Cancelled":                     "CANCELLED",
    "CancellationCode":              "CANCELLATION_CODE",
    "Diverted":                      "DIVERTED",
    "CRSElapsedTime":                "CRS_ELAPSED_TIME",
    "ActualElapsedTime":             "ELAPSED_TIME",
    "AirTime":                       "AIR_TIME",
    "Distance":                      "DISTANCE",
    "CarrierDelay":                  "DELAY_DUE_CARRIER",
    "WeatherDelay":                  "DELAY_DUE_WEATHER",
    "NASDelay":                      "DELAY_DUE_NAS",
    "SecurityDelay":                 "DELAY_DUE_SECURITY",
    "LateAircraftDelay":             "DELAY_DUE_LATE_AIRCRAFT",
    "Tail_Number":                   "Tail_Number",          # keep as-is

    # Internal DB name fallbacks (in case BTS returns raw column names
    # instead of display names — observed on some TranStats versions)
    "FL_DATE":               "FL_DATE",
    "OP_UNIQUE_CARRIER":     "AIRLINE_CODE",
    "OP_CARRIER_AIRLINE_ID": "DOT_CODE",
    "OP_CARRIER_FL_NUM":     "FL_NUMBER",
    "ORIGIN_AIRPORT_ID":     "OriginAirportID",
    "DEST_AIRPORT_ID":       "DestAirportID",
    "ORIGIN_CITY_NAME":      "ORIGIN_CITY",
    "DEST_CITY_NAME":        "DEST_CITY",
    "CRS_DEP_TIME":          "CRS_DEP_TIME",
    "DEP_TIME":              "DEP_TIME",
    "DEP_DELAY":             "DEP_DELAY",
    "TAXI_OUT":              "TAXI_OUT",
    "WHEELS_OFF":            "WHEELS_OFF",
    "WHEELS_ON":             "WHEELS_ON",
    "TAXI_IN":               "TAXI_IN",
    "CRS_ARR_TIME":          "CRS_ARR_TIME",
    "ARR_TIME":              "ARR_TIME",
    "ARR_DELAY":             "ARR_DELAY",
    "CANCELLED":             "CANCELLED",
    "CANCELLATION_CODE":     "CANCELLATION_CODE",
    "DIVERTED":              "DIVERTED",
    "CRS_ELAPSED_TIME":      "CRS_ELAPSED_TIME",
    "ACTUAL_ELAPSED_TIME":   "ELAPSED_TIME",
    "AIR_TIME":              "AIR_TIME",
    "DISTANCE":              "DISTANCE",
    "CARRIER_DELAY":         "DELAY_DUE_CARRIER",
    "WEATHER_DELAY":         "DELAY_DUE_WEATHER",
    "NAS_DELAY":             "DELAY_DUE_NAS",
    "SECURITY_DELAY":        "DELAY_DUE_SECURITY",
    "LATE_AIRCRAFT_DELAY":   "DELAY_DUE_LATE_AIRCRAFT",
    "TAIL_NUM":              "Tail_Number",
}

# Final column order in output files
OUTPUT_COLUMNS: List[str] = [
    "FL_DATE",
    "AIRLINE_CODE",
    "DOT_CODE",
    "FL_NUMBER",
    "OriginAirportID",
    "DestAirportID",
    "ORIGIN_CITY",
    "DEST",
    "DEST_CITY",
    "CRS_DEP_TIME",
    "DEP_TIME",
    "DEP_DELAY",
    "TAXI_OUT",
    "WHEELS_OFF",
    "WHEELS_ON",
    "TAXI_IN",
    "CRS_ARR_TIME",
    "ARR_TIME",
    "ARR_DELAY",
    "CANCELLED",
    "CANCELLATION_CODE",
    "DIVERTED",
    "CRS_ELAPSED_TIME",
    "ELAPSED_TIME",
    "AIR_TIME",
    "DISTANCE",
    "DELAY_DUE_CARRIER",
    "DELAY_DUE_WEATHER",
    "DELAY_DUE_NAS",
    "DELAY_DUE_SECURITY",
    "DELAY_DUE_LATE_AIRCRAFT",
    "Tail_Number",
]

MONTH_NAMES: Dict[int, str] = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}

# Polite delay between HTTP requests to avoid hammering BTS servers
REQUEST_DELAY_SECONDS: int = 3
MAX_RETRIES: int = 3
RETRY_BACKOFF_BASE_SECONDS: int = 15


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(verbose: bool = False) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    # Force UTF-8 on Windows stdout to avoid UnicodeEncodeError on
    # non-ASCII characters in log messages when piping output.
    import io
    stream = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    ) if hasattr(sys.stdout, "buffer") else sys.stdout
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=level,
        stream=stream,
    )
    return logging.getLogger("bts_downloader")


# ─────────────────────────────────────────────────────────────────────────────
# PAGE INTERACTION
# ─────────────────────────────────────────────────────────────────────────────

def _extract_hidden_fields(soup: BeautifulSoup) -> Dict[str, str]:
    """Extract all ASP.NET hidden form fields from the page."""
    result = {}
    for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION",
                 "__EVENTTARGET", "__EVENTARGUMENT", "__LASTFOCUS", "affiliate"):
        el = soup.find("input", {"name": name})
        result[name] = el.get("value", "") if el else ""
    return result


def get_page_state(session: requests.Session) -> Dict:
    """
    GET the BTS TranStats page and extract ASP.NET form state tokens.

    Returns a dict with keys: soup, __VIEWSTATE, __VIEWSTATEGENERATOR,
    __EVENTVALIDATION, affiliate — ready to embed in the next POST.
    """
    resp = session.get(BTS_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    state = _extract_hidden_fields(soup)
    state["soup"] = soup
    return state


def parse_latest_available(soup: BeautifulSoup) -> Tuple[int, int]:
    """
    Parse 'Latest Available Data: February 2026' from the page header.
    Returns (year: int, month_number: int).
    Raises ValueError if the text cannot be found or parsed.
    """
    text = soup.find(string=lambda t: t and "Latest Available Data" in t)
    if not text:
        raise ValueError(
            "Could not find 'Latest Available Data' on the BTS page. "
            "The site layout may have changed."
        )

    match = re.search(r"Latest Available Data:\s*(\w+)\s+(\d{4})", str(text))
    if not match:
        raise ValueError(
            f"Could not parse month/year from: {text!r}\n"
            "Expected format: 'Latest Available Data: February 2026'"
        )

    month_str = match.group(1)
    year = int(match.group(2))
    month_to_num = {v: k for k, v in MONTH_NAMES.items()}
    month_num = month_to_num.get(month_str)
    if month_num is None:
        raise ValueError(
            f"Unrecognised month name '{month_str}' in latest available date."
        )

    return year, month_num


def postback_year_change(
    session: requests.Session,
    year: int,
    current_state: Dict,
    logger: logging.Logger,
) -> Dict:
    """
    Submit an ASP.NET PostBack to change the year dropdown.

    When you change cboYear on the BTS form, the browser triggers
    __doPostBack('cboYear', '') which refreshes the page with a new
    __VIEWSTATE and __EVENTVALIDATION tied to the selected year. Without
    this PostBack, the subsequent download POST is rejected with an HTML
    error page (ASP.NET EVENTVALIDATION mismatch).

    Returns updated form state dict with fresh viewstate tokens.
    """
    data = {
        "__EVENTTARGET":        "cboYear",
        "__EVENTARGUMENT":      "",
        "__LASTFOCUS":          "",
        "__VIEWSTATE":          current_state["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": current_state["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION":    current_state["__EVENTVALIDATION"],
        "affiliate":            current_state.get("affiliate", ""),
        "cboGeography":         "All",
        "cboYear":              str(year),
        "cboPeriod":            "1",
    }

    logger.debug("PostBack: year change -> %d", year)
    resp = session.post(
        FORM_POST_URL,
        data=data,
        headers={**REQUEST_HEADERS, "Referer": BTS_URL},
        timeout=30,
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    state = _extract_hidden_fields(soup)
    state["soup"] = soup

    # Sanity check: verify the year change took effect
    year_select = soup.find("select", {"name": "cboYear"})
    if year_select:
        selected_opt = year_select.find("option", selected=True)
        if selected_opt and selected_opt.get("value") != str(year):
            logger.warning(
                "Year PostBack may not have taken effect. "
                "Expected %d, form shows %s.",
                year, selected_opt.get("value"),
            )

    return state


def download_month_zip(
    session: requests.Session,
    year: int,
    month: int,
    form_state: Dict,
    checkboxes: List[str],
    logger: logging.Logger,
) -> bytes:
    """
    POST the BTS download form for a specific year/month with selected fields.

    The server responds with a ZIP file containing a single CSV named like:
      On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2022_3.csv

    Returns raw ZIP bytes.
    Raises ValueError if the server returns HTML (error) instead of ZIP.
    """
    data = {
        "__EVENTTARGET":        "",
        "__EVENTARGUMENT":      "",
        "__LASTFOCUS":          "",
        "__VIEWSTATE":          form_state["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": form_state["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION":    form_state["__EVENTVALIDATION"],
        "affiliate":            form_state.get("affiliate", ""),
        "cboGeography":         "All",
        "cboYear":              str(year),
        "cboPeriod":            str(month),
        "btnDownload":          "Download",
        # chkDownloadZip is intentionally NOT included:
        # including it downloads a pre-built ZIP with ALL fields.
        # Omitting it lets us specify exact checkboxes for a smaller download.
    }

    # Add each desired field checkbox (unchecked checkboxes are not POSTed
    # in HTML forms; only checked ones are included)
    for field in checkboxes:
        data[field] = "on"

    logger.debug(
        "Downloading %d-%02d with %d fields selected",
        year, month, len(checkboxes),
    )

    resp = session.post(
        FORM_POST_URL,
        data=data,
        headers={**REQUEST_HEADERS, "Referer": BTS_URL},
        timeout=180,  # Large files can take time
        stream=True,
    )
    resp.raise_for_status()

    content = b""
    for chunk in resp.iter_content(chunk_size=65536):
        content += chunk

    if not content:
        raise ValueError(f"Empty response for {year}-{month:02d}")

    # Detect HTML error response (ASP.NET ViewState mismatch or no data)
    snippet = content[:300].lower()
    if b"<html" in snippet or b"<!doc" in snippet or b"<?xml" in snippet:
        # Try to extract an error message from the HTML
        error_soup = BeautifulSoup(content[:4096], "lxml")
        error_text = error_soup.get_text(separator=" ", strip=True)[:300]
        raise ValueError(
            f"Server returned HTML instead of ZIP for {year}-{month:02d}. "
            f"This usually means the ViewState is stale or no data exists "
            f"for that month. Error snippet: {error_text!r}"
        )

    # Validate ZIP magic bytes (PK header)
    if content[:2] != b"PK":
        raise ValueError(
            f"Response for {year}-{month:02d} is not a valid ZIP file. "
            f"Got first bytes: {content[:10].hex()}"
        )

    logger.debug(
        "Received ZIP for %d-%02d: %.1f MB",
        year, month, len(content) / 1024 / 1024,
    )
    return content


# ─────────────────────────────────────────────────────────────────────────────
# DATA PROCESSING
# ─────────────────────────────────────────────────────────────────────────────

def extract_csv_from_zip(
    zip_bytes: bytes,
    year: int,
    month: int,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Extract the CSV file from the BTS ZIP download and return as DataFrame.

    BTS ZIPs contain exactly one CSV named like:
      On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2022_3.csv

    The last row of BTS CSVs is typically an all-null summary footer — it is
    dropped automatically.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_files:
            raise ValueError(
                f"No CSV file found in ZIP for {year}-{month:02d}. "
                f"ZIP contents: {zf.namelist()}"
            )
        csv_name = csv_files[0]
        logger.debug("Extracting: %s", csv_name)

        with zf.open(csv_name) as f:
            df = pd.read_csv(f, encoding="utf-8", low_memory=False)

    # Drop the BTS trailing footer row (all-null or empty)
    if not df.empty and df.iloc[-1].isna().all():
        df = df.iloc[:-1].copy()

    logger.debug(
        "Extracted %d rows × %d columns from %s",
        len(df), len(df.columns), csv_name,
    )
    return df


def normalise_fl_date(series: pd.Series) -> pd.Series:
    """
    Normalise FL_DATE to yyyyMMdd string format expected by the Bronze
    PySpark schema (StringType, parsed with to_date(col("FL_DATE"), "yyyyMMdd")).

    BTS TranStats returns FL_DATE as 'M/D/YYYY 12:00:00 AM' (e.g.
    '1/1/2024 12:00:00 AM'). This converts it to '20240101'.
    If the value already looks like yyyyMMdd (8 digits), it is returned as-is.
    """
    def convert(val: str) -> str:
        if pd.isna(val):
            return val
        val = str(val).strip()
        # Already yyyyMMdd format
        if len(val) == 8 and val.isdigit():
            return val
        # Parse M/D/YYYY 12:00:00 AM or M/D/YYYY
        try:
            from datetime import datetime
            for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(val, fmt).strftime("%Y%m%d")
                except ValueError:
                    continue
        except Exception:
            pass
        return val  # Return unchanged if unparseable; Bronze schema is StringType

    return series.map(convert)


def process_dataframe(
    df: pd.DataFrame,
    rename_map: Dict[str, str],
    output_columns: List[str],
    year: int,
    month: int,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Rename BTS source column headers to our schema names, normalise FL_DATE
    to yyyyMMdd, then select only the desired output columns in order.

    Columns not found in the DataFrame after renaming are logged as warnings
    but do not cause failure (allows the pipeline to continue with partial data).
    """
    original_columns = list(df.columns)
    logger.debug("BTS raw columns: %s", original_columns)

    # Apply rename map (only renames columns that exist and have a mapping)
    df = df.rename(columns=rename_map)

    # Normalise FL_DATE: BTS returns 'M/D/YYYY 12:00:00 AM'; schema expects yyyyMMdd
    if "FL_DATE" in df.columns:
        sample = str(df["FL_DATE"].iloc[0]) if not df.empty else ""
        if not (len(sample) == 8 and sample.isdigit()):
            logger.debug("Normalising FL_DATE from '%s' -> yyyyMMdd", sample)
            df["FL_DATE"] = normalise_fl_date(df["FL_DATE"])

    # Select output columns that exist in the DataFrame
    available = [c for c in output_columns if c in df.columns]
    missing = [c for c in output_columns if c not in df.columns]

    if missing:
        logger.warning(
            "%d-%02d: %d expected columns not found after rename: %s\n"
            "  → Check RENAME_MAP against actual BTS column names: %s",
            year, month, len(missing), missing, original_columns,
        )

    return df[available].copy()


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATION
# ─────────────────────────────────────────────────────────────────────────────

def generate_month_range(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
) -> List[Tuple[int, int]]:
    """Generate a list of (year, month) tuples, inclusive of both endpoints."""
    months = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def download_one_month(
    session: requests.Session,
    year: int,
    month: int,
    staging_dir: Path,
    page_state: Dict,
    logger: logging.Logger,
    force: bool = False,
) -> Optional[Path]:
    """
    Download, process, and save one month of BTS data to a staging CSV.

    Returns the path to the saved file, or None if the download failed
    after all retry attempts.

    Skips download if the staging file already exists and --force is not set.
    """
    staging_path = staging_dir / f"{year}_{month:02d}.csv"

    if staging_path.exists() and not force:
        logger.info(
            "SKIP  %d-%02d — staging file exists (use --force to re-download)",
            year, month,
        )
        return staging_path

    logger.info(
        "START %d-%02d  (%s %d)",
        year, month, MONTH_NAMES[month], year,
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            zip_bytes = download_month_zip(
                session, year, month, page_state, FORM_CHECKBOXES, logger
            )
            df = extract_csv_from_zip(zip_bytes, year, month, logger)
            df = process_dataframe(df, RENAME_MAP, OUTPUT_COLUMNS, year, month, logger)

            staging_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(staging_path, index=False)

            logger.info(
                "DONE  %d-%02d  %d rows, %d cols -> %s",
                year, month, len(df), len(df.columns), staging_path.name,
            )
            return staging_path

        except Exception as exc:
            logger.warning(
                "FAIL  %d-%02d  attempt %d/%d: %s",
                year, month, attempt, MAX_RETRIES, exc,
            )
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_BASE_SECONDS * attempt
                logger.info("Retrying in %d seconds...", wait)
                time.sleep(wait)
            else:
                logger.error(
                    "ABORT %d-%02d  all %d attempts failed. "
                    "Re-run with --month %d-%02d --force to retry.",
                    year, month, MAX_RETRIES, year, month,
                )
                return None


def refresh_year_state(
    session: requests.Session,
    year: int,
    latest_year: int,
    logger: logging.Logger,
) -> Dict:
    """
    Get a fresh ASP.NET ViewState for the given year.

    For the latest year (default page state), just GET the page.
    For older years, GET then PostBack to change the year dropdown.
    """
    logger.info("Refreshing ASP.NET ViewState for year %d...", year)
    state = get_page_state(session)

    if year != latest_year:
        # PostBack required: the page defaults to latest_year.
        # Without PostBack, the EVENTVALIDATION token is bound to latest_year
        # and the download POST for an older year will fail.
        state = postback_year_change(session, year, state, logger)

    return state


def combine_staging_files(
    staging_dir: Path,
    output_path: Path,
    logger: logging.Logger,
) -> int:
    """
    Read all monthly staging CSVs in staging_dir, concatenate them, and
    write to output_path (.parquet or .csv based on file extension).

    Returns total row count. Files are sorted by filename (YYYY_MM.csv)
    so the combined output is chronologically ordered.
    """
    csv_files = sorted(staging_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No staging CSV files found in {staging_dir}. "
            "Run without --no-combine after downloading data."
        )

    logger.info(
        "Combining %d monthly files into: %s", len(csv_files), output_path
    )

    frames = []
    for csv_path in csv_files:
        df = pd.read_csv(csv_path, low_memory=False)
        frames.append(df)
        logger.debug("  Read %s: %d rows", csv_path.name, len(df))

    combined = pd.concat(frames, ignore_index=True)
    total_rows = len(combined)
    logger.info("Combined total: {:,} rows".format(total_rows))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix == ".parquet":
        combined.to_parquet(output_path, index=False, engine="pyarrow")
    else:
        combined.to_csv(output_path, index=False)

    size_mb = output_path.stat().st_size / 1024 / 1024
    logger.info("Written: %s (%.1f MB)", output_path, size_mb)
    return total_rows


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download BTS On-Time Performance data directly from TranStats.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ingestion/bts_downloader.py
  python ingestion/bts_downloader.py --years 4 --output-dir ./data/raw/bts
  python ingestion/bts_downloader.py --output-dir /Volumes/skyops/raw/bts
  python ingestion/bts_downloader.py --month 2023-06
  python ingestion/bts_downloader.py --force
  python ingestion/bts_downloader.py --no-combine
  python ingestion/bts_downloader.py --output-format csv
        """,
    )
    parser.add_argument(
        "--years", type=int, default=4,
        help=(
            "Number of years to download, counting back from the latest "
            "available month (default: 4). The start date is calculated as "
            "latest_available_date - N years."
        ),
    )
    parser.add_argument(
        "--output-dir", type=str, default="./data/raw/bts",
        help=(
            "Root output directory. Monthly staging CSVs go in "
            "<output-dir>/staging/YYYY_MM.csv. Combined file goes in "
            "<output-dir>/. (default: ./data/raw/bts)"
        ),
    )
    parser.add_argument(
        "--month", type=str, default=None,
        metavar="YYYY-MM",
        help=(
            "Download a single month only (e.g. 2023-06). "
            "Useful for backfill or retrying a failed month. "
            "Ignores --years when set."
        ),
    )
    parser.add_argument(
        "--output-format", choices=["parquet", "csv"], default="parquet",
        help=(
            "Format for the combined output file (default: parquet). "
            "Parquet is strongly recommended for datasets > 1M rows."
        ),
    )
    parser.add_argument(
        "--force", action="store_true",
        help=(
            "Re-download and overwrite staging CSVs even if they already exist. "
            "Without --force, existing staging files are skipped (resume mode)."
        ),
    )
    parser.add_argument(
        "--no-combine", action="store_true",
        help=(
            "Skip creating the combined output file. "
            "Useful when you want monthly files only, or when running on a "
            "machine without enough RAM to hold the full dataset."
        ),
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable DEBUG level logging (shows HTTP details, column names, etc.)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = setup_logging(args.verbose)

    output_dir = Path(args.output_dir)
    staging_dir = output_dir / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)

    # ── Step 1: Connect to BTS and detect latest available month ─────────────
    logger.info("=" * 60)
    logger.info("SkyOps BTS Downloader")
    logger.info("Source: %s", BTS_URL)
    logger.info("=" * 60)

    logger.info("Connecting to BTS TranStats to detect latest available month...")
    try:
        initial_state = get_page_state(session)
    except requests.RequestException as exc:
        logger.error(
            "Failed to connect to BTS TranStats: %s\n"
            "Check network connectivity and try again.",
            exc,
        )
        sys.exit(1)

    latest_year, latest_month = parse_latest_available(initial_state["soup"])
    logger.info(
        "Latest available: %s %d",
        MONTH_NAMES[latest_month], latest_year,
    )

    # ── Step 2: Determine date range ─────────────────────────────────────────
    if args.month:
        try:
            sm_year, sm_month = map(int, args.month.split("-"))
            if not (1987 <= sm_year <= latest_year):
                raise ValueError(f"Year {sm_year} out of BTS range (1987-{latest_year})")
            if not (1 <= sm_month <= 12):
                raise ValueError(f"Month {sm_month} must be between 1 and 12")
            if (sm_year, sm_month) > (latest_year, latest_month):
                raise ValueError(
                    f"{args.month} is after the latest available "
                    f"({MONTH_NAMES[latest_month]} {latest_year})"
                )
        except (ValueError, AttributeError) as exc:
            logger.error("Invalid --month: %s", exc)
            sys.exit(1)
        month_range = [(sm_year, sm_month)]
    else:
        latest_date = date(latest_year, latest_month, 1)
        start_date = latest_date - relativedelta(years=args.years)
        month_range = generate_month_range(
            start_date.year, start_date.month,
            latest_year, latest_month,
        )

    start_y, start_m = month_range[0]
    end_y, end_m = month_range[-1]
    logger.info(
        "Download range: %s %d -> %s %d (%d months)",
        MONTH_NAMES[start_m], start_y,
        MONTH_NAMES[end_m], end_y,
        len(month_range),
    )
    logger.info("Output directory: %s", output_dir.resolve())
    logger.info("Output format: %s", args.output_format)
    logger.info("-" * 60)

    # ── Step 3: Download each month ──────────────────────────────────────────
    successful: List[Path] = []
    failed: List[Tuple[int, int]] = []

    # Track per-year state to avoid redundant PostBacks
    current_year: Optional[int] = None
    current_year_state: Optional[Dict] = None

    for i, (year, month) in enumerate(month_range):
        # Refresh ViewState on each year boundary
        if year != current_year:
            try:
                current_year_state = refresh_year_state(
                    session, year, latest_year, logger
                )
                current_year = year
            except Exception as exc:
                logger.error(
                    "Failed to get ViewState for year %d: %s. "
                    "Skipping all months in %d.",
                    year, exc, year,
                )
                failed.extend((y, m) for y, m in month_range if y == year)
                # Advance i past all months in this year
                continue

        result = download_one_month(
            session=session,
            year=year,
            month=month,
            staging_dir=staging_dir,
            page_state=current_year_state,
            logger=logger,
            force=args.force,
        )

        if result is not None:
            successful.append(result)
        else:
            failed.append((year, month))

        # Polite delay between requests (skip after the last month)
        if i < len(month_range) - 1:
            time.sleep(REQUEST_DELAY_SECONDS)

    # ── Step 4: Summary ───────────────────────────────────────────────────────
    logger.info("-" * 60)
    logger.info(
        "Download complete: %d succeeded, %d failed",
        len(successful), len(failed),
    )
    if failed:
        failed_str = ", ".join(f"{y}-{m:02d}" for y, m in failed)
        logger.warning(
            "Failed months (re-run each with --month YYYY-MM --force):\n  %s",
            failed_str,
        )

    # ── Step 5: Combine staging files into single output ─────────────────────
    if args.no_combine:
        logger.info(
            "Skipped combining (--no-combine). "
            "Monthly staging files in: %s", staging_dir,
        )
    elif successful:
        start_label = f"{month_range[0][0]}_{month_range[0][1]:02d}"
        end_label = f"{month_range[-1][0]}_{month_range[-1][1]:02d}"
        out_filename = f"bts_ontime_{start_label}_{end_label}.{args.output_format}"
        out_path = output_dir / out_filename

        try:
            total_rows = combine_staging_files(staging_dir, out_path, logger)
            logger.info(
                "Combined output: %s  ({:,} total rows)".format(total_rows),
                out_path,
            )
        except Exception as exc:
            logger.error(
                "Failed to create combined output: %s\n"
                "Monthly staging files are preserved in: %s",
                exc, staging_dir,
            )
    else:
        logger.warning(
            "No months were successfully downloaded — "
            "combined output not created."
        )

    logger.info("=" * 60)
    if failed:
        sys.exit(1)  # Non-zero exit so CI/Lakeflow Jobs detects partial failure


if __name__ == "__main__":
    main()
