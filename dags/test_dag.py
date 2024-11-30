import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime


default_args = {
    'start_date': datetime(2024, 11, 30), 
    'retries': 1, 
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    description='Testing to see if DAG loads correctly',
    schedule_interval='0 6 * * *', 
    dagrun_timeout=timedelta(minutes=5),
)


test = BashOperator(
    task_id='echo',
    bash_command='echo "Test DAG is working!"',
    dag=dag,
    depends_on_past=False,
)