from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='testing_env_variable',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
) as dag:

    # Task to retrieve and print the variable
    def print_variable():
        # Retrieve the variable
        api_key = Variable.get("openweather_api_key", default_var="Variable not found")
        print(f"Retrieved Variable: {api_key}")

    print_var_task = PythonOperator(
        task_id='print_variable',
        python_callable=print_variable,
    )