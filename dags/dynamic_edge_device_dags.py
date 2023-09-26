import csv
from pathlib import Path
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import boto3

# Define the relative path to the CSV file
CSV_PATH = Path(os.path.dirname(os.path.abspath(__file__))) / 'config' / 'conf_file.csv'


def read_edge_devices_from_csv(file_path):
    edge_devices = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            edge_devices.append(row)
    return edge_devices


# Read the edge devices from the specified CSV path
EDGE_DEVICES = read_edge_devices_from_csv(CSV_PATH)

# DAG Definitions and other configurations
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def create_dag(device):
    dag_id = f"edge_device_dag_{device['id']}"

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


def print_edge_device_name(device_id, **kwargs):
    print(f"Processing data for edge device: {device_id}")


for device in EDGE_DEVICES:
    dag = create_dag(device)
    globals()[dag.dag_id] = dag
