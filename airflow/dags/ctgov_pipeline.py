# airflow/dags/ctgov_pipeline.py
# -----------------------------------------------------------------------------
# ClinicalTrials.gov (API v2) -> Postgres (warehouse) via Airflow
#
# Tasks:
#  - fetch_trials: paginate API v2, normalize JSON into tabular rows, and load
#                  using PostgreSQL COPY into a staging table + UPSERT to final
#  - deduplicate_raw: (safety) ensures a deduplicated copy (by nct_id)
#
# Notes:
#  - We keep dates as ISO strings in raw tables (dbt will parse/validate)
#  - location_country is a '|' separated list of unique country names
#  - COPY is used instead of pandas.to_sql for speed + robustness
# -----------------------------------------------------------------------------

from __future__ import annotations

from datetime import datetime, timedelta
import os
import time
from io import StringIO

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils_db import get_engine  # our helper -> SQLAlchemy engine

# ----------------------------
# API configuration (v2)
# ----------------------------
API_BASE = "https://clinicaltrials.gov/api/v2/studies"

# You can tune these without changing code (set as Airflow env vars if you want)
QUERY_CONDITION = os.getenv("CTGOV_QUERY_CONDITION", "cancer")
QUERY_LOCATION = os.getenv("CTGOV_QUERY_LOCATION", "Portugal OR Europe")
PAGE_SIZE = int(os.getenv("CTGOV_PAGE_SIZE", "200"))           # API max 1000, 200 is fine
MAX_PAGES = os.getenv("CTGOV_MAX_PAGES")                       # None = no cap; or "5" to cap
SLEEP_BETWEEN_PAGES = float(os.getenv("CTGOV_SLEEP_SEC", "0")) # throttle if needed

# Fields we will extract from the JSON (nested)
# We are not requesting "fields=" filter; v2 returns full JSON for each study.
# If you want to reduce payload, you can add "fields=" in params with dotted paths.
def extract_nct_id(study: dict) -> str | None:
    return (
        study.get("protocolSection", {})
        .get("identificationModule", {})
        .get("nctId")
    )

def extract_brief_title(study: dict) -> str | None:
    return (
        study.get("protocolSection", {})
        .get("identificationModule", {})
        .get("briefTitle")
    )

def extract_overall_status(study: dict) -> str | None:
    return (
        study.get("protocolSection", {})
        .get("statusModule", {})
        .get("overallStatus")
    )

def extract_study_type(study: dict) -> str | None:
    return (
        study.get("protocolSection", {})
        .get("designModule", {})
        .get("studyType")
    )

def extract_start_date(study: dict) -> str | None:
    # Keep as ISO string; dbt will cast with validation
    dt = (
        study.get("protocolSection", {})
        .get("statusModule", {})
        .get("startDateStruct", {})
        .get("date")
    )
    return dt if isinstance(dt, str) else None

def extract_last_update_post_date(study: dict) -> str | None:
    dt = (
        study.get("protocolSection", {})
        .get("statusModule", {})
        .get("lastUpdateSubmitDate")
    )
    return dt if isinstance(dt, str) else None

def extract_condition(study: dict) -> str:
    # Join multiple conditions with "; "
    conds = (
        study.get("protocolSection", {})
        .get("conditionsModule", {})
        .get("conditions", [])
    ) or []
    if isinstance(conds, list):
        txt = "; ".join([c for c in conds if c])
        return txt[:1000]
    return str(conds)[:1000]

def extract_countries(study: dict) -> str:
    # Collect unique countries from contactsLocationsModule.locations[].country
    locs = (
        study.get("protocolSection", {})
        .get("contactsLocationsModule", {})
        .get("locations", [])
    ) or []
    countries: list[str] = []
    for loc in locs:
        if not isinstance(loc, dict):
            continue
        c = (loc.get("country") or "").strip()
        if c:
            countries.append(c)
    # Make unique + stable order
    countries = sorted(set(countries))
    return "|".join(countries)

def fetch_trials(**context):
    """
    Pull all pages from API v2 using nextPageToken, flatten into rows (dicts),
    load into staging via COPY, then UPSERT into final raw_ctgov.
    """
    params = {
        # v2 query grammar: using lexeme fields is complex; the "query.term" param is the simple search
        # We'll include both condition and location in the term:
        "query.term": f"{QUERY_CONDITION} ({QUERY_LOCATION})",
        "pageSize": PAGE_SIZE,
        "format": "json",
        "countTotal": "true",
    }

    all_rows: list[dict] = []
    next_token = None
    pages = 0
    max_pages_int = int(MAX_PAGES) if MAX_PAGES and MAX_PAGES.isdigit() else None

    while True:
        page_params = dict(params)
        if next_token:
            # In v2, you must pass pageToken (the response returns nextPageToken)
            page_params["pageToken"] = next_token

        r = requests.get(API_BASE, params=page_params, timeout=60)
        # Raise clear error if Bad Request (e.g., malformed query.term)
        r.raise_for_status()
        payload = r.json()

        studies = payload.get("studies", []) or []
        rows_this_page: list[dict] = []
        for s in studies:
            rows_this_page.append({
                "nct_id": extract_nct_id(s),
                "condition": extract_condition(s),
                "brief_title": extract_brief_title(s),
                "overall_status": extract_overall_status(s),
                "study_type": extract_study_type(s),
                "start_date": extract_start_date(s),
                "location_country": extract_countries(s),
                "last_update_post_date": extract_last_update_post_date(s),
            })

        all_rows.extend(rows_this_page)

        next_token = payload.get("nextPageToken")
        pages += 1
        if max_pages_int is not None and pages >= max_pages_int:
            break
        if not next_token:
            break
        if SLEEP_BETWEEN_PAGES > 0:
            time.sleep(SLEEP_BETWEEN_PAGES)

    # Build DataFrame
    df = pd.DataFrame(all_rows, columns=[
        "nct_id", "condition", "brief_title", "overall_status",
        "study_type", "start_date", "location_country", "last_update_post_date"
    ])

    # Load to Postgres via COPY (staging -> upsert into final)
    engine = get_engine()
    with engine.begin() as conn:
        # Final table with PK
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS raw_ctgov (
              nct_id text PRIMARY KEY,
              condition text,
              brief_title text,
              overall_status text,
              study_type text,
              start_date text,
              location_country text,
              last_update_post_date text,
              _loaded_at timestamptz DEFAULT now()
            );
        """)

        # Staging (no PK)
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS raw_ctgov_load (
              nct_id text,
              condition text,
              brief_title text,
              overall_status text,
              study_type text,
              start_date text,
              location_country text,
              last_update_post_date text
            );
        """)
        conn.exec_driver_sql("TRUNCATE TABLE raw_ctgov_load;")

        if not df.empty:
            # Prepare CSV buffer (no header)
            buf = StringIO()
            df.to_csv(buf, index=False, header=False)
            buf.seek(0)

            # Use the underlying psycopg2 connection for COPY
            raw = conn.connection.connection  # SQLAlchemy -> psycopg2
            with raw.cursor() as cur:
                cur.copy_expert(
                    """
                    COPY raw_ctgov_load
                    (nct_id, condition, brief_title, overall_status, study_type,
                     start_date, location_country, last_update_post_date)
                    FROM STDIN WITH (FORMAT CSV)
                    """,
                    buf
                )

            # Idempotent UPSERT into final
            conn.exec_driver_sql("""
                INSERT INTO raw_ctgov (
                  nct_id, condition, brief_title, overall_status, study_type,
                  start_date, location_country, last_update_post_date
                )
                SELECT
                  nct_id, condition, brief_title, overall_status, study_type,
                  start_date, location_country, last_update_post_date
                FROM raw_ctgov_load
                ON CONFLICT (nct_id) DO UPDATE SET
                  condition = EXCLUDED.condition,
                  brief_title = EXCLUDED.brief_title,
                  overall_status = EXCLUDED.overall_status,
                  study_type = EXCLUDED.study_type,
                  start_date = EXCLUDED.start_date,
                  location_country = EXCLUDED.location_country,
                  last_update_post_date = EXCLUDED.last_update_post_date,
                  _loaded_at = now();
            """)


def deduplicate_raw(**context):
    """
    Keep a deduplicated snapshot of raw_ctgov by (nct_id), preferring latest _loaded_at.
    This is mostly a safety/view convenience; the main idempotency is the UPSERT.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS raw_ctgov_dedup AS
            SELECT * FROM raw_ctgov WHERE 1=0;
        """)
        conn.exec_driver_sql("TRUNCATE TABLE raw_ctgov_dedup;")
        conn.exec_driver_sql("""
            INSERT INTO raw_ctgov_dedup
            SELECT DISTINCT ON (nct_id) *
            FROM raw_ctgov
            ORDER BY nct_id, _loaded_at DESC;
        """)


# ----------------------------
# Airflow DAG definition
# ----------------------------
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ctgov_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    description="Ingest ClinicalTrials.gov (API v2) into Postgres warehouse (raw + dedup).",
    tags=["clinicaltrials", "oncology", "ingestion"],
) as dag:

    t_fetch = PythonOperator(
        task_id="fetch_trials",
        python_callable=fetch_trials,
    )

    t_dedup = PythonOperator(
        task_id="deduplicate_raw",
        python_callable=deduplicate_raw,
    )

    t_fetch >> t_dedup
