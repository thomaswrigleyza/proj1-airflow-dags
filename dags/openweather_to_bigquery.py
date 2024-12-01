import requests
import json
import os
import time
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

cities = [
    # South Africa
    {"city_short_code": "ZA-CPT", "lat": 33.9221, "lon": 18.4231},  # Cape Town
    {"city_short_code": "ZA-JHB", "lat": -26.2041, "lon": 28.0473},  # Johannesburg
    {"city_short_code": "ZA-DBN", "lat": -29.8587, "lon": 31.0218},  # Durban
    {"city_short_code": "ZA-PLZ", "lat": -33.9575, "lon": 25.6022},  # Port Elizabeth
    {"city_short_code": "ZA-BFN", "lat": -29.0852, "lon": 26.1596},  # Bloemfontein
    {"city_short_code": "ZA-PMB", "lat": -29.6168, "lon": 30.3928},  # Pietermaritzburg
    {"city_short_code": "ZA-NLP", "lat": -23.8962, "lon": 29.4486},  # Polokwane
    {"city_short_code": "ZA-ELS", "lat": -32.9833, "lon": 27.8667},  # East London
    {"city_short_code": "ZA-RTB", "lat": -25.6701, "lon": 27.2411},  # Rustenburg
    {"city_short_code": "ZA-UPN", "lat": -25.7479, "lon": 28.2293},  # Pretoria

    # North America
    {"city_short_code": "US-NYC", "lat": 40.7128, "lon": -74.0060},  # New York City, USA
    {"city_short_code": "US-LAX", "lat": 34.0522, "lon": -118.2437},  # Los Angeles, USA
    {"city_short_code": "US-CHI", "lat": 41.8781, "lon": -87.6298},  # Chicago, USA
    {"city_short_code": "US-HOU", "lat": 29.7604, "lon": -95.3698},  # Houston, USA
    {"city_short_code": "CA-TOR", "lat": 43.6511, "lon": -79.3835},  # Toronto, Canada
    {"city_short_code": "CA-VAN", "lat": 49.2827, "lon": -123.1207},  # Vancouver, Canada
    {"city_short_code": "MX-MEX", "lat": 19.4326, "lon": -99.1332},  # Mexico City, Mexico

    # South America
    {"city_short_code": "BR-RIO", "lat": -22.9068, "lon": -43.1729},  # Rio de Janeiro, Brazil
    {"city_short_code": "BR-SPA", "lat": -23.5505, "lon": -46.6333},  # São Paulo, Brazil
    {"city_short_code": "AR-BUE", "lat": -34.6037, "lon": -58.3816},  # Buenos Aires, Argentina
    {"city_short_code": "CL-SAN", "lat": -33.4489, "lon": -70.6693},  # Santiago, Chile
    {"city_short_code": "CO-BOG", "lat": 4.7110, "lon": -74.0721},  # Bogotá, Colombia

    # Europe
    {"city_short_code": "GB-LON", "lat": 51.5074, "lon": -0.1278},  # London, UK
    {"city_short_code": "FR-PAR", "lat": 48.8566, "lon": 2.3522},  # Paris, France
    {"city_short_code": "DE-BER", "lat": 52.5200, "lon": 13.4050},  # Berlin, Germany
    {"city_short_code": "IT-ROM", "lat": 41.9028, "lon": 12.4964},  # Rome, Italy
    {"city_short_code": "ES-MAD", "lat": 40.4168, "lon": -3.7038},  # Madrid, Spain
    {"city_short_code": "RU-MOW", "lat": 55.7558, "lon": 37.6173},  # Moscow, Russia
    {"city_short_code": "GR-ATH", "lat": 37.9838, "lon": 23.7275},  # Athens, Greece
    {"city_short_code": "NL-AMS", "lat": 52.3676, "lon": 4.9041},  # Amsterdam, Netherlands
    {"city_short_code": "SE-STO", "lat": 59.3293, "lon": 18.0686},  # Stockholm, Sweden
    {"city_short_code": "PL-WAR", "lat": 52.2297, "lon": 21.0122},  # Warsaw, Poland

    # Asia
    {"city_short_code": "CN-BJ", "lat": 39.9042, "lon": 116.4074},  # Beijing, China
    {"city_short_code": "IN-MUM", "lat": 19.0760, "lon": 72.8777},  # Mumbai, India
    {"city_short_code": "JP-TKO", "lat": 35.6895, "lon": 139.6917},  # Tokyo, Japan
    {"city_short_code": "KR-SEL", "lat": 37.5665, "lon": 126.9780},  # Seoul, South Korea
    {"city_short_code": "SG-SIN", "lat": 1.3521, "lon": 103.8198},  # Singapore
    {"city_short_code": "TH-BKK", "lat": 13.7563, "lon": 100.5018},  # Bangkok, Thailand
    {"city_short_code": "VN-HAN", "lat": 21.0285, "lon": 105.8542},  # Hanoi, Vietnam
    {"city_short_code": "MY-KUL", "lat": 3.1390, "lon": 101.6869},  # Kuala Lumpur, Malaysia
    {"city_short_code": "PH-MNL", "lat": 14.5995, "lon": 120.9842},  # Manila, Philippines
    {"city_short_code": "PK-KHI", "lat": 24.8607, "lon": 67.0011},  # Karachi, Pakistan

    # Middle East
    {"city_short_code": "SA-RYD", "lat": 24.7136, "lon": 46.6753},  # Riyadh, Saudi Arabia
    {"city_short_code": "IR-THR", "lat": 35.6892, "lon": 51.3890},  # Tehran, Iran
    {"city_short_code": "AE-DXB", "lat": 25.276987, "lon": 55.296249},  # Dubai, UAE
    {"city_short_code": "TR-IST", "lat": 41.0082, "lon": 28.9784},  # Istanbul, Turkey
    {"city_short_code": "IL-TLV", "lat": 32.0853, "lon": 34.7818},  # Tel Aviv, Israel

    # Africa
    {"city_short_code": "NG-LOS", "lat": 6.5244, "lon": 3.3792},  # Lagos, Nigeria
    {"city_short_code": "NG-ABJ", "lat": 9.0765, "lon": 7.3986},  # Abuja, Nigeria
    {"city_short_code": "KE-NBI", "lat": -1.2864, "lon": 36.8172},  # Nairobi, Kenya
    {"city_short_code": "EG-CAI", "lat": 30.0444, "lon": 31.2357},  # Cairo, Egypt
    {"city_short_code": "DZ-ALG", "lat": 36.7372, "lon": 3.0865},  # Algiers, Algeria
    {"city_short_code": "ET-ADD", "lat": 9.0306, "lon": 38.7407},  # Addis Ababa, Ethiopia
    {"city_short_code": "GH-ACC", "lat": 5.6037, "lon": -0.1870},  # Accra, Ghana
    {"city_short_code": "MA-CAS", "lat": 33.5731, "lon": -7.5898},  # Casablanca, Morocco
    {"city_short_code": "TN-TUN", "lat": 36.8065, "lon": 10.1815},  # Tunis, Tunisia
    {"city_short_code": "SD-KRT", "lat": 15.5007, "lon": 32.5599},  # Khartoum, Sudan

    # Oceania
    {"city_short_code": "AU-SYD", "lat": -33.8688, "lon": 151.2093},  # Sydney, Australia
    {"city_short_code": "AU-MEL", "lat": -37.8136, "lon": 144.9631},  # Melbourne, Australia
    {"city_short_code": "NZ-AKL", "lat": -36.8485, "lon": 174.7633},  # Auckland, New Zealand
]

with DAG(
    dag_id='openweather_to_bigquery',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='0 6 * * *',
    catchup=False,
) as dag:

   
    def fetch_openweather_data(**kwargs):
        api_key = Variable.get("openweather_api_key")
        rows = []

        for city in cities:
            url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={city['lat']}&lon={['lon']}&appid={api_key}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json  
        
            for pollutant, value in data["list"][0]["components"].items():
                row = {
                    "city_short_code": city["city_short_code"],
                    "lon": data["coord"]["lon"],
                    "lat": data["coord"]["lat"],
                    "aqi": data["list"][0]["main"]["aqi"],
                    "pollutant": pollutant,
                    "value": value,
                    "dt": data["list"][0]["dt"],
                    "last_updated": datetime.fromtimestamp(data["list"][0]["dt"]).strftime('%Y-%m-%d %H:%M:%S'),
                }
                rows.append(row)
            
            # OpenWeather's API rate limits at 60 requests / minute.
            time.sleep(2)
    
        kwargs['ti'].xcom_push(key='rows', value=rows)

    fetch_data = PythonOperator(
        task_id='fetch_openweather_data',
        python_callable=fetch_openweather_data,
        provide_context=True,
    )

    def insert_data_to_bigquery(**kwargs):
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