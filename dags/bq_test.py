from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='test_bigquery_connection',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, 
    catchup=False,
) as dag:

    test_bq_query = BigQueryInsertJobOperator(
        task_id='run_test_query',
        configuration={
            "query": {
                "query": "SELECT * FROM `utility-replica-441110-u8.openweather_data.src_openweather_aqi` LIMIT 10",
                "useLegacySql": False,
            }
        },
        location="US",
    )