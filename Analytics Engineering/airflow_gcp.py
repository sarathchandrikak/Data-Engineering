from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'gcs_to_bq_pipeline',
    default_args=default_args,
    description='A DAG to read data from GCS and write to BigQuery',
    schedule_interval='@daily',  # Run DAG daily
)

# Define tasks
start_task = DummyOperator(task_id='start_task', dag=dag)

# Define a Python function to read data from GCS
def read_data_from_gcs():
    gcs_hook = GoogleCloudStorageHook()
    data = gcs_hook.download('gs://your-bucket/your-file.csv')
    return data

read_data_task = PythonOperator(
    task_id='read_data_task',
    python_callable=read_data_from_gcs,
    dag=dag,
)

# Define a task to load data into BigQuery
load_to_bq_task = GoogleCloudStorageToBigQueryOperator(
    task_id='load_to_bq_task',
    bucket='your-bucket',
    source_objects=['your-file.csv'],
    destination_project_dataset_table='your_project.your_dataset.your_table',
    write_disposition='WRITE_TRUNCATE',  # Overwrite table if it exists
    skip_leading_rows=1,  # Skip header row
    schema_fields=[{'name': 'field1', 'type': 'STRING'}, {'name': 'field2', 'type': 'INTEGER'}],  # Define schema
    dag=dag,
)

# Define task dependencies
start_task >> read_data_task >> load_to_bq_task
