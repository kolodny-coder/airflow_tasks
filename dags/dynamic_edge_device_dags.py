import csv
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import boto3

# Path to the local CSV file
LOCAL_CSV_PATH = '/usr/local/airflow/dags/edge_devices.csv'
print(f"Looking for CSV file at: {LOCAL_CSV_PATH}")

# S3 bucket and object key for the CSV file
S3_BUCKET = 'dank-airflow-demo'
S3_KEY = 'config/edge_devices.csv'

def read_edge_devices_from_csv(file_path):
    edge_devices = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            edge_devices.append(row)
    return edge_devices

def download_file_from_s3(bucket, key, local_path):
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, local_path)

# Check if the script is running in a cloud environment
cloud_environment = os.environ.get('CLOUD_ENVIRONMENT')
print(cloud_environment)
if 'CLOUD_ENVIRONMENT' in os.environ:
    # Download the CSV file from S3 to a local path
    download_file_from_s3(S3_BUCKET, S3_KEY, LOCAL_CSV_PATH)

# Read the edge devices from the local CSV file
EDGE_DEVICES = read_edge_devices_from_csv(LOCAL_CSV_PATH)

def print_edge_device_name(device_id, **kwargs):
    print(f"Processing data for edge device: {device_id}")

def create_dag(device):
    dag_id = f"edge_device_dag_{device['id']}"
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'DAG for Edge Device {device["id"]}',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 22),
        catchup=False,
    )
    with dag:
        start_task = DummyOperator(task_id='start')
        print_device_task = PythonOperator(
            task_id='print_device_name',
            python_callable=print_edge_device_name,
            op_args=[device['id']],
            provide_context=True,
        )
        end_task = DummyOperator(task_id='end')
        start_task >> print_device_task >> end_task
    return dag

for device in EDGE_DEVICES:
    dag = create_dag(device)
    globals()[dag.dag_id] = dag
