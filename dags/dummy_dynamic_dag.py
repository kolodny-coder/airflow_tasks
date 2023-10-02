from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_s3 import S3ToS3Operator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from dags.utils  import get_edge_devices
from dags.utils import report_status
from dags.utils import check_s3_key
from dags.utils  import trigger_device

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1)
}


def _create_task_group(dag, device):
    with TaskGroup(group_id=f"device_{device['name']}") as tg:
        trigger_device_task = PythonOperator(
            task_id="trigger_device",
            python_callable=trigger_device,
            op_args=[device["id"]]
        )

        report_trigger_task = PythonOperator(
            task_id="report_trigger",
            python_callable=report_status,
            op_args=["trigger_device"],
            provide_context=True,
        )

        check_s3_key_task = PythonOperator(
            task_id="check_s3_key",
            python_callable=check_s3_key,
            op_args=[
                f"{device['s3_key_prefix']}/{{ds}}/{device['name']}.json",
                "my_bucket"
            ]
        )

        report_s3_key_task = PythonOperator(
            task_id="report_s3_key",
            python_callable=report_status,
            op_args=["check_s3_key"],
            provide_context=True,
        )

        copy_s3_file_task = S3ToS3Operator(
            task_id="copy_s3_file",
            source_bucket_key=...,
            dest_bucket_key=...,
            source_bucket_name="my_bucket",
            dest_bucket_name="my_bucket",
            aws_conn_id="aws_default"
        )

        report_copy_task = PythonOperator(...)

        return tg


def create_dag():
    dag = DAG(
        "device_etl",
        default_args=default_args,
        schedule_interval="@daily"
    )

    with dag:
        start = DummyOperator(task_id="start")

        devices = get_edge_devices()

        task_groups = []
        for device in devices:
            tg = _create_task_group(dag, device)
            task_groups.append(tg)

        end = DummyOperator(task_id="end")

        start >> task_groups >> end

    return dag


dag = create_dag()