from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
import pandas as pd
import requests
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 12),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('vault_openweather_to_s3', default_args=default_args, schedule_interval="@once", catchup=False)

# Set your OpenWeather API endpoint
api_endpoint = "https://api.openweathermap.org/data/2.5/forecast"

# Path to the location.json file
location_file_path = './location.json'  

def get_vault_secret(secret_path):
    vault_hook = VaultHook()
    secret = vault_hook.get_secret(secret_path)
    return secret

def extract_weather_data(**kwargs):
    ti = kwargs['ti']
    
    # Retrieve the API key from Vault
     
    weather_api_key = get_vault_secret(secret_path)['api_key']
    
    # Load locations from JSON file
    with open(location_file_path, 'r') as f:
        locations = json.load(f)
    
    all_data = []
    
    for location in locations:
        lat = location['lat']
        lon = location['lon']
        
        # Prepare the API call parameters
        api_params = {
            "lat": lat,
            "lon": lon,
            "appid": weather_api_key
        }
        
        # Make the API call
        response = requests.get(api_endpoint, params=api_params)
        data = response.json()
        
        # Normalize the JSON data and add location details
        df = pd.json_normalize(data['list'])
        df['latitude'] = lat
        df['longitude'] = lon
        all_data.append(df)
    
    # Combine data for all locations into one DataFrame
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Push the DataFrame to XCom as CSV
    ti.xcom_push(key='weather_data_csv', value=final_df.to_csv(index=False))

def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    weather_data_csv = ti.xcom_pull(task_ids='extract_weather_data', key='weather_data_csv')
    
    # S3 upload task
    s3_operator = S3CreateObjectOperator(
        task_id="upload_to_s3",
        aws_conn_id='aws_default',
        s3_bucket='weather-data-gds',
        s3_key='date={{ ds }}/weather_data.csv',
        data=weather_data_csv,
        dag=dag
    )
    
    s3_operator.execute(kwargs)

extract_weather_data_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    provide_context=True,
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3_task',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

extract_weather_data_task >> upload_to_s3_task