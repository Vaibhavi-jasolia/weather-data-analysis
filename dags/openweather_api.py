from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.transfers.local_to_blob import LocalToAzureBlobOperator
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG('openweather_to_azure_blob',
          default_args=default_args,
          schedule_interval='@hourly',
          catchup=False)

# Load locations from a JSON file
def load_locations(filename='locations.json'):
    with open(filename, 'r') as file:
        return json.load(file)

# List of coordinates (latitude, longitude) to retrieve weather data for
locations = load_locations()

# Azure Key Vault details
key_vault_name = "$(weather-data-analysis)"
key_vault_uri = f"https://{key_vault_name}.vault.azure.net"

# Retrieve secrets from Azure Key Vault
def get_secret(secret_name):
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_uri, credential=credential)
    secret = client.get_secret(secret_name)
    return secret.value

# OpenWeather API key from Key Vault
api_key = get_secret("openweather-api-key")

# Azure Blob Storage details from Key Vault
azure_blob_storage_connection_string = get_secret("azure-blob-storage-connection-string")

# Function to extract weather data
def extract_weather_data(**kwargs):
    weather_data = []
    for loc in locations:
        response = requests.get("https://api.openweathermap.org/data/2.5/weather", params={
            "lat": loc['lat'],
            "lon": loc['lon'],
            "appid": api_key,
            "units": "metric"
        })
        if response.status_code == 200:
            data = response.json()
            data['lat'] = loc['lat']
            data['lon'] = loc['lon']
            weather_data.append(data)
        else:
            print(f"Failed to fetch data for {loc['lat']}, {loc['lon']}")

    # Convert data to DataFrame and save as Parquet
    df = pd.json_normalize(weather_data)
    file_name = f"weather_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    file_path = f"/tmp/{file_name}"
    df.to_parquet(file_path, index=False)
    
    # Push file path to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='file_path', value=file_path)

# Define the task to extract weather data
extract_data = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    provide_context=True,
    dag=dag
)

# Define the task to upload to Azure Blob Storage
upload_to_azure_blob = LocalToAzureBlobOperator(
    task_id='upload_to_azure_blob',
    src_file="{{ ti.xcom_pull(task_ids='extract_weather_data', key='file_path') }}",
    dest_blob='weather_data/{{ ds }}/{{ execution_date.strftime("%Y%m%d%H%M%S") }}.parquet',
    container_name='weather-data',
    azure_blob_storage_conn_id='azure_blob_default',
    dag=dag
)

# Task dependencies
extract_data >> upload_to_azure_blob
