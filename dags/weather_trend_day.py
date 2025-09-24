import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "1ltan"
DAG_ID = "weather_trend_day"
LAYER = "raw"
SOURCE = "weather"
SCHEMA = "dm"
TARGET_TABLE = "weather_trend_day"
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 9, 24, tz="Europe/Kyiv"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["dm", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="from_s3_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,
        poke_interval=60,
    )

    drop_stg_table_before = SQLExecuteQueryOperator(
        task_id="drop_stg_table_before",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
        WITH prev_day AS (
            SELECT
                total_cases::float AS prev_temp,
                new_cases::float AS prev_precip
            FROM ods.fct_weather
            WHERE date = '{{{{ data_interval_start - pendulum.duration(days=1).format('YYYY-MM-DD') }}}}' 
            LIMIT 1
        ),
        curr_day AS (
            SELECT
                total_cases::float AS curr_temp,
                new_cases::float AS curr_precip
            FROM ods.fct_weather
            WHERE date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}' 
            LIMIT 1
        )
        SELECT
            '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}' AS date,
            c.curr_temp,
            c.curr_precip,
            CASE 
                WHEN p.prev_temp IS NOT NULL AND c.curr_temp > p.prev_temp THEN 'Rising'
                WHEN p.prev_temp IS NOT NULL AND c.curr_temp < p.prev_temp THEN 'Falling'
                ELSE 'Stable'
            END AS temp_trend,
            CASE 
                WHEN p.prev_precip IS NOT NULL AND c.curr_precip > p.prev_precip THEN 'Increasing'
                WHEN p.prev_precip IS NOT NULL AND c.curr_precip < p.prev_precip THEN 'Decreasing'
                ELSE 'Stable'
            END AS precip_trend
        FROM curr_day c
        LEFT JOIN prev_day p ON TRUE;
        """,
    )

    drop_from_target_table = SQLExecuteQueryOperator(
        task_id="drop_from_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE date IN
        (
            SELECT date FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        )
        """,
    )

    insert_into_target_table = SQLExecuteQueryOperator(
        task_id="insert_into_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id="drop_stg_table_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    end = EmptyOperator(
        task_id="end",
    )

    (
        start >>
        sensor_on_raw_layer >>
        drop_stg_table_before >>
        create_stg_table >>
        drop_from_target_table >>
        insert_into_target_table >>
        drop_stg_table_after >>
        end
    )