import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "1ltan"
DAG_ID = "from_api_to_s3"

LAYER = "raw"
SOURCE = "weather"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Kyiv"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    """"""

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

    locations = [
        {"name": "Berlin"},
        {"name": "Rome"},
        {"name": "London"},
    ]

    unions = []
    for loc in locations:
        url = f"https://api.open-meteo.com/v1/archive?latitude={loc['lat']}&longitude={loc['lon']}&start_date={start_date}&end_date={start_date}&daily=temperature_2m_mean,precipitation_sum,wind_speed_10m_max,relative_humidity_2m_mean&timezone=Europe/Kyiv"
        unions.append(f"""
            SELECT
                daily_time[0]::date as date,
                'UA'::varchar as iso_code,
                'Europe'::varchar as continent,
                '{loc['name']}'::varchar as location,
                daily_temperature_2m_mean[0]::double as total_cases,
                daily_precipitation_sum[0]::double as new_cases,
                daily_wind_speed_10m_max[0]::double as total_deaths,
                0::integer as new_deaths,
                daily_relative_humidity_2m_mean[0]::double as new_vaccinations,
                1000000::bigint as population
            FROM read_json_auto('{url}')
        """)

    select_sql = " UNION ALL ".join(unions)

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY
        (
            {select_sql}
        ) TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet' (FORMAT PARQUET, COMPRESSION GZIP);
        """
    )

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end