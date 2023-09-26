import csv
from pathlib import Path
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import requests

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


def trigger_edge_device_request(device_id, **context):
    url = "http://34.234.78.46:5000/addjob"
    headers = {"Content-Type": "application/json"}
    task_id = f"task-{context['ts_nodash']}"
    data = {
        "deviceNo": device_id,
        "start_time_stamp": "2023-09-22T10:00:00",
        "end_time_stamp": "2023-09-22T11:00:00",
        "s3Bucket": "dank-airflow",
        "prefix": "some-prefix",
        "message": "Sample message",
        "taskId": task_id
    }
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()


def create_dag(device):
    dag_id = f"edge_device_dag_{device['name']}"

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'DAG for Edge Device {device["name"]}',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 22),
        catchup=False,
    )

    with dag:
        trigger_edge_device_task = PythonOperator(
            task_id='trigger_edge_device',
            python_callable=trigger_edge_device_request,
            op_args=[device['name']],
            provide_context=True,
            dag=dag,
        )

        s3_key_sensor_task = S3KeySensor(
            task_id='s3_key_sensor_task',
            bucket_key=f'some-prefix/{device["name"]}/{{{{ ds }}}}/task-{{{{ ts_nodash }}}}_{device["name"]}.json',
            bucket_name='dank-airflow',
            aws_conn_id='connect_to_s3_dank_account',
            poke_interval=60,
            timeout=3600,
            mode='poke',
            dag=dag,
        )

        copy_s3_file_task = S3CopyObjectOperator(
            task_id='copy_s3_file_task',
            source_bucket_key=f'some-prefix/{device["name"]}/{{{{ ds }}}}/task-{{{{ ts_nodash }}}}_{device["name"]}.json',
            dest_bucket_key=f'after-copy/{device["name"]}/{{{{ ds }}}}/task-{{{{ ts_nodash }}}}_{device["name"]}.json',
            source_bucket_name='dank-airflow',
            dest_bucket_name='dank-airflow',
            aws_conn_id='connect_to_s3_dank_account',
            dag=dag,
        )

        trigger_edge_device_task >> s3_key_sensor_task >> copy_s3_file_task

    return dag


for device in EDGE_DEVICES:
    dag = create_dag(device)
    globals()[dag.dag_id] = dag