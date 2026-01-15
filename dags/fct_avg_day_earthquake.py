import pendulum
from airflow                                    import DAG
from airflow.operators.empty                    import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task              import ExternalTaskSensor

# переменные из метаданных AIRFLOW
if True:
    # Конфигурация DAG
    OWNER             = "V.Gorenkov"
    DAG_ID            = "fct_avg_day_earthquake"

    LONG_DESCRIPTION  = """
    # LONG DESCRIPTION
    """

    SHORT_DESCRIPTION = "SHORT DESCRIPTION"

    args = {
    "owner"      : OWNER,
    "start_date" : pendulum.datetime(2025, 5 , 1, tz = "Europe/Moscow"),
    "catchup"    : True,
    "retries"    : 3,
    "retry_delay": pendulum.duration(hours = 1),
}

# Другие переменные
if True:
    # Используемые таблицы в DAG
    LAYER             = "raw"
    SOURCE            = "earthquake"
    SCHEMA            = "dm"
    TARGET_TABLE      = "fct_avg_day_earthquake"

    # DWH
    PG_CONNECT        = "postgres_dwh"


with DAG(
    dag_id            = DAG_ID,
    schedule_interval = "0 5 * * *",
    default_args      = args,
    tags              = ["dm", "pg"],
    description       = SHORT_DESCRIPTION,
    concurrency       = 1,
    max_active_tasks  = 1,
    max_active_runs   = 1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id         = "sensor_on_raw_layer",
        external_dag_id = "raw_from_s3_to_pg",
        allowed_states  = ["success"],
        mode            = "reschedule",
        timeout         = 360000, # длительность работы сенсора
        poke_interval   = 60,     # частота проверки
    )

    drop_stg_table_before_sql    = f"""DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
                                    """
    create_stg_table_sql         = f"""CREATE TABLE         stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
                                       SELECT time::date AS date,
                                              avg(mag::float)
                                       FROM   ods.fct_earthquake
                                       WHERE  time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
                                       GROUP BY 1
                                    """
    drop_from_target_table_sql   = f"""DELETE FROM {SCHEMA}.{TARGET_TABLE}
                                       WHERE date IN
                                       (
                                           SELECT date FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
                                       )
                                    """
    insert_into_target_table_sql = f"""INSERT INTO {SCHEMA}.{TARGET_TABLE}
                                       SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
                                    """
    drop_stg_table_after_sql     = f"""DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
                                    """

    (
        EmptyOperator          (task_id         = "start")                                                                                                 >>
        ExternalTaskSensor     (task_id         = "sensor_on_raw_layer", 
                                external_dag_id = "raw_from_s3_to_pg", allowed_states  = ["success"], 
                                                                       mode            = "reschedule", 
                                                                       timeout         = 360000, # длительность работы сенсора
                                                                       poke_interval   = 60,     # частота проверки
                                )                                                                                                                          >>
        SQLExecuteQueryOperator(task_id         = "drop_stg_table_before",    conn_id = PG_CONNECT, autocommit = True, sql = drop_stg_table_before_sql)    >>
        SQLExecuteQueryOperator(task_id         = "create_stg_table",         conn_id = PG_CONNECT, autocommit = True, sql = create_stg_table_sql)         >>
        SQLExecuteQueryOperator(task_id         = "drop_from_target_table",   conn_id = PG_CONNECT, autocommit = True, sql = drop_from_target_table_sql)   >>
        SQLExecuteQueryOperator(task_id         = "insert_into_target_table", conn_id = PG_CONNECT, autocommit = True, sql = insert_into_target_table_sql) >>
        SQLExecuteQueryOperator(task_id         = "drop_stg_table_after",     conn_id = PG_CONNECT, autocommit = True, sql = drop_stg_table_after_sql)     >>
        EmptyOperator          (task_id         = "end")
    )