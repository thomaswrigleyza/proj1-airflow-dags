from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def test_keyfile_access():
    try:
        with open("/Users/thomaswrigley/Code/Personal/InsightEngine/Keys/airflow_bq_key.json") as f:
            print("Keyfile contents:")
            print(f.read())
    except Exception as e:
        raise ValueError(f"Error accessing keyfile: {e}")

with DAG(
    dag_id='test_path',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    test_access = PythonOperator(
        task_id='test_path',
        python_callable=test_keyfile_access,
    )