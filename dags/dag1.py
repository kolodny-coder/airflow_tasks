from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.operators.python_operator import PythonOperator
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_key_sensor_dag',
    default_args=default_args,
    description='A DAG to trigger an edge device, check for a file in S3, and copy it if it exists',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 22),
    catchup=False,
)

def trigger_edge_device_request():
    url = "http://34.234.78.46:5000/addjob"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "deviceNo": "12345",
        "start_time_stamp": "2023-09-22T10:00:00",
        "end_time_stamp": "2023-09-22T11:00:00",
        "s3Bucket": "dank-airflow",
        "prefix": "some-prefix",
        "message": "Sample message",
        "taskId": "task-003"
    }
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()  # This will raise an exception if the request returned an error

trigger_edge_device_task = PythonOperator(
    task_id='trigger_edge_device',
    python_callable=trigger_edge_device_request,
    dag=dag,
)

s3_key_sensor_task = S3KeySensor(
    task_id='s3_key_sensor_task',
    bucket_key='some-prefix/2023-09-23/task-003.json',
    bucket_name='dank-airflow',
    aws_conn_id='connect_to_s3_dank_account',
    poke_interval=60,
    timeout=3600,
    mode='poke',
    dag=dag,
)

copy_s3_file_task = S3CopyObjectOperator(
    task_id='copy_s3_file_task',
    source_bucket_key='some-prefix/2023-09-23/task-003.json',
    dest_bucket_key='after-copy/2023-09-23/task-003.json',
    source_bucket_name='dank-airflow',
    dest_bucket_name='dank-airflow',
    aws_conn_id='connect_to_s3_dank_account',
    dag=dag,
)

trigger_edge_device_task >> s3_key_sensor_task >> copy_s3_file_task
