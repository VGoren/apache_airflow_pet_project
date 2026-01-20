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
    SOURCE            = "wikimedia.org"
    PROJECT_NAME_1    = "en.wikipedia.org"
    PROJECT_NAME_2    = "en.wikipedia.org"
    PAGE_NAME_1       = "Santa_Claus"
    PAGE_NAME_2       = "Coca-Cola"
    # S3
    ACCESS_KEY        = Variable.get("access_key")
    SECRET_KEY        = Variable.get("secret_key")
    LONG_DESCRIPTION  = """
    # LONG DESCRIPTION
    """
    SHORT_DESCRIPTION = "SHORT DESCRIPTION"
    args              = {
                            "owner"      : OWNER,
                            "start_date" : pendulum.datetime(2020, 1, 1, tz = "Europe/Moscow"),
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
    start_date = context["data_interval_start"].format("YYYYMMDD")
    end_date   = context["data_interval_end"]  .format("YYYYMMDD")
    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    """"""
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()
    # пользовался https://shell.duckdb.org/ для отладки
    sql = f"""
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
            WITH 
            desktop    AS (SELECT unnest(items::JSON[]) AS item FROM read_json('https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{PROJECT_NAME_1}/desktop/all-agents/{PAGE_NAME_1}/daily/{start_date}/{end_date}',    format='unstructured')),
            mobile_web AS (SELECT unnest(items::JSON[]) AS item FROM read_json('https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{PROJECT_NAME_1}/mobile-web/all-agents/{PAGE_NAME_1}/daily/{start_date}/{end_date}', format='unstructured')),
            mobile_app AS (SELECT unnest(items::JSON[]) AS item FROM read_json('https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{PROJECT_NAME_1}/mobile-app/all-agents/{PAGE_NAME_1}/daily/{start_date}/{end_date}', format='unstructured'))
            SELECT strptime(desktop.item.timestamp, '"%Y%m%d%H"') AS timestamp,
                   desktop.   item.views::INT                     AS desktop_views, 
                   mobile_web.item.views::INT                     AS mobile_web_views, 
                   mobile_app.item.views::INT                     AS mobile_app_views,
                   desktop.   item.views::INT +                     
                   mobile_web.item.views::INT +                   
                   mobile_app.item.views::INT                     AS all_access_views
            FROM      desktop
            LEFT JOIN mobile_web ON desktop.item.timestamp = mobile_web.item.timestamp
            LEFT JOIN mobile_app ON desktop.item.timestamp = mobile_app.item.timestamp
        ) TO 's3://{minio_bucket}/{LAYER}/{SOURCE}/{PROJECT_NAME_1}/{PAGE_NAME_1}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """

    con.sql(sql)
    sql = sql.replace(PROJECT_NAME_1, PROJECT_NAME_2)
    sql = sql.replace(PAGE_NAME_1,    PAGE_NAME_2)
    con.sql(sql)

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
    schedule_interval = "0 5 1 * *",      # каждый первый день месяца в 5 утра
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