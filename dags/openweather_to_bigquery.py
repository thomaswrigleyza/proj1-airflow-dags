from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
import requests
import json
from airflow.models import Variable
api_key = Variable.get("OPENWEATHER_API_KEY")



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='openweather_to_bigquery',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # Will update to @daily in next iteration
    catchup=False,
) as dag:

    # Testing OpenWeather API request for Cape Town only. Will update method to fetch for multiple cities in next iteration
    def fetch_openweather_data(**kwargs):
        api_key = Variable.get("openweather_api_key")
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat=33.9221&lon=18.4231&appid={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()


        rows = []
        for pollutant, value in data["list"][0]["components"].items():
            row = {
                "lon": data["coord"]["lon"],
                "lat": data["coord"]["lat"],
                "aqi": data["list"][0]["main"]["aqi"],
                "pollutant": pollutant,
                "value": value,
                "dt": data["list"][0]["dt"],
            }
            rows.append(row)
        
    
        kwargs['ti'].xcom_push(key='rows', value=rows)

    fetch_data = PythonOperator(
        task_id='fetch_openweather_data',
        python_callable=fetch_openweather_data,
        provide_context=True,
    )

    def insert_data_to_bigquery(**kwargs):
        from google.cloud import bigquery
        client = bigquery.Client()

        table_id = "utility-replica-441110-u8.openweather_data.src_openweather_aqi"
        rows = kwargs['ti'].xcom_pull(key='rows', task_ids='fetch_openweather_data')
        
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            raise Exception(f"Failed to insert rows: {errors}")

    insert_data = PythonOperator(
        task_id='insert_data_to_bigquery',
        python_callable=insert_data_to_bigquery,
        provide_context=True,
    )

    fetch_data >> insert_data