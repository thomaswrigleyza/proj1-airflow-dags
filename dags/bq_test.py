from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
from airflow.models import Variable
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def test_bigquery():
    # Construct keyfile JSON from Airflow Variables
    keyfile_data = {
        "type": Variable.get("gcp_type"),
        "project_id": Variable.get("gcp_project_id"),
        "private_key_id": Variable.get("gcp_private_key_id"),
        "private_key": Variable.get("gcp_private_key").replace("\\n", "\n"),
        "client_email": Variable.get("gcp_client_email"),
        "client_id": Variable.get("gcp_client_id"),
        "auth_uri": Variable.get("gcp_auth_uri"),
        "token_uri": Variable.get("gcp_token_uri"),
        "auth_provider_x509_cert_url": Variable.get("gcp_auth_provider_x509_cert_url"),
        "client_x509_cert_url": Variable.get("gcp_client_x509_cert_url"),
        "universe_domain": Variable.get("gcp_universe_domain")
    }
    
    # Authenticate with the keyfile
    keyfile_path = "/tmp/airflow_gcp_key.json"
    with open(keyfile_path, "w") as keyfile:
        json.dump(keyfile_data, keyfile)

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