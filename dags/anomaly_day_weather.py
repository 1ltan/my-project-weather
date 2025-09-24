import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "1ltan"
DAG_ID = "anomaly_day_weather"
LAYER = "raw"
SOURCE = "weather"
SCHEMA = "dm"
TARGET_TABLE = "anomaly_day_weather"
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
        WITH stats AS (
            SELECT
                date,
                AVG(total_cases::float) AS avg_temp,
                STDDEV(total_cases::float) AS std_temp,
                AVG(new_cases::float) AS avg_precip,
                STDDEV(new_cases::float) AS std_precip
            FROM ods.fct_weather
            WHERE date <= '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}' 
            AND date > '{{{{ data_interval_start - pendulum.duration(days=7).format('YYYY-MM-DD') }}}}'
            GROUP BY date
        ),
        current_data AS (
            SELECT
                date,
                total_cases::float AS curr_temp,
                new_cases::float AS curr_precip
            FROM ods.fct_weather
            WHERE date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
        )
        SELECT
            c.date,
            c.curr_temp,
            c.curr_precip,
            CASE WHEN s.avg_temp IS NOT NULL AND s.std_temp IS NOT NULL 
                 AND (c.curr_temp - s.avg_temp) / s.std_temp > 2 
                 THEN TRUE ELSE FALSE END AS is_temp_anomaly,
            CASE WHEN s.avg_precip IS NOT NULL AND s.std_precip IS NOT NULL 
                 AND (c.curr_precip - s.avg_precip) / s.std_precip > 2 
                 THEN TRUE ELSE FALSE END AS is_precip_anomaly
        FROM current_data c
        LEFT JOIN stats s ON c.date = s.date;
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