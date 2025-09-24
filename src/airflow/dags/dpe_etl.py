"""
ETL DAG for DPE dataset (Diagnostics de Performance Energétique).
This DAG fetches energy performance diagnostic records from the OpenDataSoft API,
processes them through an ETL pipeline, and saves to CSV.
Demonstrates task dependencies and data flow in Airflow.
"""

import json
import pendulum
from airflow.sdk import dag, task, Asset
from pendulum import datetime
import requests
import pandas as pd
import os
from typing import List, Dict
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# API Configuration
URL = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/base-des-diagnostics-de-performance-energetique-dpe-des-batiments-non-residentie/records"
PARAMS = {
    "limit": 5,
    "offset": 0,
    "timezone": "UTC",
    "include_links": "false",
    "include_app_metas": "false"
}
HEADERS = {"accept": "application/json; charset=utf-8"}


@dag(
    start_date=datetime(2025, 9, 22),
    schedule="@daily",
    doc_md=__doc__,
    default_args={
        "owner": "Faical",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["etl", "dpe", "energy", "opendata"],
    catchup=False,
    max_active_runs=1,
)
def dpe_etl_pipeline():

    @task()
    def extract_dpe_records(**context) -> List[Dict]:
        """
        EXTRACT: Fetch raw DPE records from OpenDataSoft API.
        Saves JSON.
        """
        # URL of the API
        url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/base-des-diagnostics-de-performance-energetique-dpe-des-batiments-non-residentie/records"
        params = {
            "limit": 10,
            "offset": 0,
            "timezone": "UTC",
            "include_links": "false",
            "include_app_metas": "false"
        }

        # Headers
        headers = {
            "accept": "application/json; charset=utf-8"
        }

        # Make GET request
        response = requests.get(url, headers=headers, params=params)
        print(f"Response Status Code: {response.status_code}")

        return response.json()
        

    @task
    def transform_dpe_records(raw_dpe_records, **context) -> pd.DataFrame:
        """
        TRANSFORM: Clean and structure the raw DPE data.
        Reads JSON, converts to DataFrame, applies transformations,
        and returns a DataFrame.
        """
        df = pd.DataFrame(raw_dpe_records.get("results", []))

        print(f"Original DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")

        # Example transformation
        df = df.drop_duplicates()
        df["processed_at"] = pendulum.now("UTC").to_iso8601_string()

        return df   # ✅ Return the actual DataFrame

    @task
    def load_dpe_records(transform_dpe_records_result: pd.DataFrame):
        """
        Load transformed data into Postgres using pandas.to_sql.
        Creates table automatically if it does not exist.
        """
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dpe_conn")
        
        # ✅ Use SQLAlchemy engine instead of psycopg2 conn
        engine = postgres_hook.get_sqlalchemy_engine()
        print(f"SQLAlchemy Engine: {engine}")
        # # Write DataFrame to Postgres (creates table if not exists)
        # from sqlalchemy import create_engine
        # engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')
        transform_dpe_records_result.to_sql('dpe_data', engine)

        # transform_dpe_records_result.to_sql(
        #     name="dpe_data",
        #     con=engine,
        #     if_exists="append",   # don't overwrite existing
        #     index=False
        # )
        print("Data loaded into Postgres successfully.")
        # # Log results
        with engine.connect() as conn:
            total_count = conn.execute("SELECT COUNT(*) FROM dpe_data;").scalar()
        print(f"✅ Loaded {len(transform_dpe_records_result)} records. 📊 Total in DB: {total_count}")

    # Define task dependencies
    raw_dpe_records = extract_dpe_records()
    transform_dpe_records_result = transform_dpe_records(raw_dpe_records)
    load_dpe_records(transform_dpe_records_result)

# Instantiate the DAG
dpe_etl = dpe_etl_pipeline()
