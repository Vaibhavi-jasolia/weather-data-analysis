from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'load_and_transform_data',
    default_args=default_args,
    description='Load data from Azure Blob Storage to Snowflake and transform using dbt',
    schedule_interval='@hourly',  
    catchup=False,
)

# Define Snowflake connection ID
snowflake_conn_id = 'snowflake_default'

# Define the task to load data from Azure Blob Storage into Snowflake
load_data_task = SnowflakeOperator(
    task_id='load_data_from_blob_to_snowflake',
    snowflake_conn_id=snowflake_conn_id,
    sql="""
    COPY INTO table_name
    FROM 'azure://weather-analysis@weather-analysis.blob.core.windows.net/path/to/your/files/'
    CREDENTIALS=(AZURE_SAS_TOKEN='$(sas_token)')
    FILE_FORMAT=(TYPE=PARQUET)
    ON_ERROR='CONTINUE';
    """,
    dag=dag,
)

# Define the task to run dbt transformations
transform_data_task = DbtCloudRunJobOperator(
    task_id='run_dbt_transformations',
    dbt_cloud_conn_id='dbt_cloud_default',  
    job_id=12345,  
    dag=dag,
)

# Set task dependencies
load_data_task >> transform_data_task

