# utils.py

import csv
import requests

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_edge_devices():
    devices = []
    with open('devices.csv') as f:
        reader = csv.DictReader(f)
        for row in reader:
            devices.append(row)
    return devices


def trigger_device(device_id):
    url = f"http://api.com/devices/{device_id}/trigger"
    response = requests.post(url)
    if response.status_code != 200:
        raise Exception("API error")


def check_s3_key(key, bucket_name, **context):
    hook = S3Hook(aws_conn_id="aws_default")
    found = hook.check_for_key(key, bucket_name)
    if not found:
        raise ValueError("Key not found")


def get_status_from_context(context):
    ti = context['task_instance']

    if ti.state == 'success':
        return 'success'

    if ti.state == 'failed':
        return 'failed'

    # Check if failure was propagated via XCom
    trigger_failed = ti.xcom_pull(task_ids='trigger_device', key='failure')
    s3_failed = ti.xcom_pull(task_ids='check_s3', key='failure')

    if trigger_failed or s3_failed:
        return 'failed'

    # Otherwise return the task's current state
    return ti.state


def report_status(task_name, **context):
    hook = PostgresHook(postgres_conn_id="postgres")
    status = get_status_from_context(context)
    execution_date = context['execution_date']

    sql = f"INSERT INTO task_status (task, status, execution_dt) VALUES (%s, %s, %s)"
    hook.run(sql, parameters=[task_name, status, execution_date])