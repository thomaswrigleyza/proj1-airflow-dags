from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
from datetime import timedelta
import requests
import json
import os
from airflow.models import Variable


api_key = Variable.get("openweather_api_key")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='env_variable_test',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # Will update to @daily in next iteration
    catchup=False,
) as dag:

    # Testing OpenWeather API request for Cape Town only. Will update method to fetch for multiple cities in next iteration
    def fetch_openweather_data(**kwargs):
        api_key = os.environ.getenv("OPENWEATHER_API_KEY")
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat=33.9221&lon=18.4231&appid={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    
        return response