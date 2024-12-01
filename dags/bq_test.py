from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def test_bigquery():
    # Authenticate with the keyfile
    keyfile_path = "/Users/thomaswrigley/Code/Personal/InsightEngine/Keys/airflow_bq_key.json"
    credentials = service_account.Credentials.from_service_account_file(keyfile_path)

    # Initialize BigQuery client
    client = bigquery.Client(credentials=credentials, project="utility-replica-441110-u8")

    # Run a query
    query = "SELECT * FROM `utility-replica-441110-u8.openweather_data.src_openweather_aqi` LIMIT 10"
    results = client.query(query).result()

    for row in results:
        print(row)

with DAG(
    dag_id='test_bigquery_python_auth',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    python_task = PythonOperator(
        task_id='run_python_query',
        python_callable=test_bigquery,
    )