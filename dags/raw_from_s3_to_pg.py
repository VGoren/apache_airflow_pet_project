import logging
import duckdb
import pendulum
from airflow                       import DAG
from airflow.models                import Variable
from airflow.operators.empty       import EmptyOperator
from airflow.operators.python      import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

# переменные из метаданных AIRFLOW
if True:
    # Конфигурация DAG
    OWNER  = "V.Gorenkov"
    DAG_ID = "raw_from_s3_to_pg"

    # Используемые таблицы в DAG
    LAYER          = "raw"
    SOURCE         = "wikimedia.org"
    PROJECT_NAME_1 = "en.wikipedia.org"
    PROJECT_NAME_2 = "en.wikipedia.org"
    PAGE_NAME_1    = "Santa_Claus"
    PAGE_NAME_2    = "Coca-Cola"
    SCHEMA         = "ods"
    TARGET_TABLE   = "fct_views"

    # S3
    ACCESS_KEY = Variable.get("access_key")
    SECRET_KEY = Variable.get("secret_key")

    # DuckDB
    PASSWORD = Variable.get("pg_password")

    LONG_DESCRIPTION = """
    # LONG DESCRIPTION
    """

    SHORT_DESCRIPTION = "SHORT DESCRIPTION"

    args = {
        "owner"      : OWNER,
        "start_date" : pendulum.datetime(2020, 1, 1, tz = "Europe/Moscow"),
        "catchup"    : True,
        "retries"    : 3,
        "retry_delay": pendulum.duration(hours = 1),
    }

if True:
    minio_bucket = 'main'

def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYYMMDD")
    end_date   = context["data_interval_end"]  .format("YYYYMMDD")
    return start_date, end_date


def get_and_transfer_raw_data_to_ods_pg(**context):
    """"""
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()
    con.sql(f"""
        INSTALL httpfs;
        LOAD    httpfs;
        SET TIMEZONE             = 'UTC';
        SET s3_url_style         = 'path';
        SET s3_endpoint          = 'minio:9000';
        SET s3_access_key_id     = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl           = FALSE;

        CREATE SECRET dwh_postgres (
            TYPE      postgres,
            HOST     'postgres_dwh',
            PORT      5432,
            DATABASE  postgres,
            USER     'postgres',
            PASSWORD '{PASSWORD}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
        """)

    sql = f"""
        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}(timestamp, page_id, desktop_views, mobile_web_views, mobile_app_views, all_access_views)
        SELECT                                              timestamp, page_id, desktop_views, mobile_web_views, mobile_app_views, all_access_views
        FROM 's3://{minio_bucket}/{LAYER}/{SOURCE}/{PROJECT_NAME_1}/{PAGE_NAME_1}/{start_date}/{start_date}_00-00-00.gz.parquet',
             postgres_query('dwh_postgres_db', $$SELECT {SCHEMA}.upsert_page('{SCHEMA}', '{PROJECT_NAME_1}', '{PAGE_NAME_1}') AS page_id$$);
        """
    
    logging.info(f"✅ QUERY: {sql}")
    con.sql(sql)
    sql = sql.replace(PROJECT_NAME_1, PROJECT_NAME_2)
    sql = sql.replace(PAGE_NAME_1,    PAGE_NAME_2)
    logging.info(f"✅ QUERY: {sql}")
    con.sql(sql)

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")

with DAG(
    dag_id            = DAG_ID,
    schedule_interval = "0 5 1 * *",
    default_args      = args,
    tags              = ["s3", "ods", "pg"],
    description       = SHORT_DESCRIPTION,
    concurrency       = 1,
    max_active_tasks  = 1,
    max_active_runs   = 1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    (
        EmptyOperator     (task_id         = "start")                                                                      >> 
        ExternalTaskSensor(task_id         = "sensor_on_raw_layer", 
                           external_dag_id = "raw_from_api_to_s3", allowed_states  = ["success"], 
                                                                   mode            = "reschedule",
                                                                   timeout         = 360000, # длительность работы сенсора 
                                                                   poke_interval   = 60,     # частота проверки
                            )                                                                                              >>
        PythonOperator    (task_id         = "get_and_transfer_raw_data_to_ods_pg",
                           python_callable =  get_and_transfer_raw_data_to_ods_pg)                                         >> 
        EmptyOperator     (task_id         = "end")
    )