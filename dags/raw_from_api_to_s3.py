import logging
import duckdb
import pendulum                                     #для работы с датами
from airflow                  import DAG
from airflow.models           import Variable
from airflow.operators.empty  import EmptyOperator  #оператор заглушка
from airflow.operators.python import PythonOperator #исполнитель python кода

# переменные из метаданных AIRFLOW
if True:
    # Конфигурация DAG
    OWNER             = "V.Gorenkov"
    DAG_ID            = "raw_from_api_to_s3"
    # Используемые таблицы в DAG
    LAYER             = "raw"
    SOURCE            = "earthquake"
    # S3
    ACCESS_KEY        = Variable.get("access_key")
    SECRET_KEY        = Variable.get("secret_key")
    LONG_DESCRIPTION  = """
    # LONG DESCRIPTION
    """
    SHORT_DESCRIPTION = "SHORT DESCRIPTION"
    args              = {
                            "owner"      : OWNER,
                            "start_date" : pendulum.datetime(2025 , 5  , 1, tz = "Europe/Moscow"),
                            #retries (попытки повторного выполнения) — это встроенный механизм, позволяющий автоматически повторять выполнение упавшей задачи (таска) заданное количество раз с определенной задержкой перед тем, как отметить ее как окончательно провалившуюся, что обеспечивает отказоустойчивость и надежность ваших конвейеров данных (DAGs).
                            "retries"    : 3,
                            # Если catchup=True, Airflow выполнит все пропущенные интервалы, а при catchup=False пропустит их, начиная выполнение только с текущей даты.
                            "catchup"    : True,
                            "retry_delay": pendulum.duration(hours = 1),
                        }
# Другие переменные
if True:
    minio_bucket = 'main'

'''
В Apache Airflow контекст (context) — это словарь (dictionary)
с метаданными и переменными выполнения, который становится доступен внутри задач (tasks) и DAG во время их работы, 
позволяя получать информацию о текущем запуске (dag_run), логической дате (logical_date), статусе задачи (task_instance) и 
другой среде, что важно для шаблонизации с помощью Jinja, передачи параметров и взаимодействия через XCom.
'''
def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date   = context["data_interval_end"]  .format("YYYY-MM-DD")
    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    """"""
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

    con.sql(
        f"""
        INSTALL httpfs;
        LOAD    httpfs;
        SET TIMEZONE             = 'UTC';
        SET s3_url_style         = 'path';
        SET s3_endpoint          = 'minio:9000';
        SET s3_access_key_id     = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl           = FALSE;

        COPY
        (
            SELECT
                *
            FROM
                read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
        ) TO 's3://{minio_bucket}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")

# Инициализация DAG'а
with DAG(
    dag_id            = DAG_ID,
                                          #| | | | |
                                          #| | | | +----- День недели (0-7, где 0/7 - воскресенье)
                                          #| | | +------- Месяц (1-12)
                                          #| | +--------- День месяца (1-31)
                                          #| +----------- Часы (0-23)
                                          #+------------- Минуты (0-59)
                                          #* - каждый
    schedule_interval = "0 5 * * *",      # каждый день в 5 утра
    default_args      = args,
    tags              = ["s3", "raw"],
    description       = SHORT_DESCRIPTION,
    concurrency       = 1,                # исключение параллелизма
    max_active_tasks  = 1,                # исключение параллелизма
    max_active_runs   = 1,                # исключение параллелизма
) as dag:
    dag.doc_md = LONG_DESCRIPTION

(
    EmptyOperator (task_id         = "start")                           >> 
    PythonOperator(task_id         = "get_and_transfer_api_data_to_s3",
                    python_callable =  get_and_transfer_api_data_to_s3)  >> 
    EmptyOperator (task_id         = "end")
)