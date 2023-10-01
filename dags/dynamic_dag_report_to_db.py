from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable, TaskInstance
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import csv
import requests
import time


POSTGRES_CONN_ID = 'postgres_status_report'
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'conf_file.csv')

def read_edge_devices_from_csv(file_path):
    edge_devices = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            edge_devices.append(row)
    return edge_devices


EDGE_DEVICES = read_edge_devices_from_csv(CSV_PATH)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}


def some_task(**context):
    task_id = context['task_instance'].task_id
    execution_date = context.get('logical_date', context['execution_date'])
    dag_id = context['dag'].dag_id

    # Generate a unique run_id using dag_id and execution_date
    run_id = f"{dag_id}_{execution_date.isoformat()}"

    report_status_to_db(task_id, run_id, **context)


def report_status_to_db(task_id: str, run_id: str, **context):
    time.sleep(3)
    status_mapping = {
        "trigger_edge_device": "running_on_edge_device",
        "s3_key_sensor_task": "polling_for_file",
        "copy_s3_file_task": "success"
    }

    dag_id = context['dag'].dag_id
    execution_date = context.get('logical_date', context['execution_date'])
    execution_date_str = execution_date.isoformat() if execution_date else None

    ti: TaskInstance = context['task_instance']

    # Pull the XCom to check whether the trigger_edge_device_task has failed
    trigger_edge_device_failed = ti.xcom_pull(task_ids='trigger_edge_device', key='trigger_edge_device_failed')
    copy_s3_file_failed = ti.xcom_pull(task_ids='copy_s3_file_task', key='copy_s3_file_failed')
    s3_key_sensor_failed = ti.xcom_pull(task_ids='s3_key_sensor_task', key='s3_key_sensor_failed')

    if trigger_edge_device_failed:
        status = 'failed'
    elif copy_s3_file_failed:
        status = 'failed'
    elif s3_key_sensor_failed:
        status = 'failed'

    else:
        status = status_mapping.get(task_id, "unknown_status")
        # If the current task instance has failed, then status is 'failed'
        if ti.state == 'failed':
            status = 'failed'

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"""
        INSERT INTO task_reports (dag_id, task_id, execution_date, status, run_id)
        VALUES (%s, %s, %s, %s, %s)
        """

    hook.run(sql, parameters=[dag_id, task_id, execution_date_str, status, run_id])

    # If status is 'failed', the reporting task itself should fail.
    if status == 'failed':
        print('Reporting task: Main task failed.')


def create_reporting_task_for(task, dag):
    return PythonOperator(
        task_id=f'report_{task.task_id}',
        python_callable=report_status_to_db,
        provide_context=True,
        op_args=[task.task_id],  # The task_id is now passed as an argument to the callable
        trigger_rule='all_done',
        dag=dag
    )


def s3_key_sensor_callable(bucket_key, bucket_name, aws_conn_id, **context):
    try:
        # Check if the task should fail
        should_fail = context['dag_run'].conf.get('fail_s3_key_sensor_task', False) if context.get('dag_run') else False
        if should_fail:

            raise ValueError("Failing task 's3_key_sensor_task' as per configuration")

        # Execute the S3KeySensor logic
        sensor = S3KeySensor(
            task_id='s3_key_sensor_task',
            bucket_key=bucket_key,
            bucket_name=bucket_name,
            aws_conn_id=aws_conn_id,
            mode='poke',
        )
    except Exception as e:
        # If the task fails, push an XCom to signal the failure
        ti: TaskInstance = context['task_instance']
        ti.xcom_push(key='s3_key_sensor_failed', value=True)
        raise e  # Re-raise the exception to ensure the task is marked as failed

    return sensor.execute(context=context)


def handle_failure(context):
    task_id = context['task_instance'].task_id
    execution_date = context.get('logical_date', context['execution_date'])
    dag_id = context['dag'].dag_id

    # Generate a unique run_id using dag_id and execution_date
    run_id = f"{dag_id}_{execution_date.isoformat()}"

    # Call the function with run_id as a keyword argument
    report_status_to_db(task_id=task_id, run_id=run_id, **context)



def trigger_edge_device_request(device_id, **context):
    # Check if the task should fail
    try:
        should_fail = context['dag_run'].conf.get('fail_trigger_edge_device_task', False) if context.get('dag_run') else False
        if should_fail:
            raise ValueError("Failing task 'trigger_edge_device_task' as per configuration")


        url = "http://34.234.78.46:5000/addjob"
        headers = {"Content-Type": "application/json"}
        task_id = f"task-{context['ts_nodash']}"

        execution_date = context.get('execution_date')
        default_start_time_stamp = execution_date - timedelta(minutes=5)
        default_end_time_stamp = execution_date

        start_time_stamp_override = context['dag_run'].conf.get('start_time_stamp') if context.get('dag_run') else None
        end_time_stamp_override = context['dag_run'].conf.get('end_time_stamp') if context.get('dag_run') else None

        start_time_stamp = start_time_stamp_override or Variable.get('start_time_stamp',
                                                                     default_start_time_stamp.isoformat())
        end_time_stamp = end_time_stamp_override or Variable.get('end_time_stamp', default_end_time_stamp.isoformat())

        data = {
            "deviceNo": device_id,
            "start_time_stamp": start_time_stamp,
            "end_time_stamp": end_time_stamp,
            "s3Bucket": "dank-airflow",
            "prefix": "some-prefix",
            "message": "Sample message",
            "taskId": task_id
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
    except Exception as e:
        # If the task fails, push an XCom to signal the failure
        ti: TaskInstance = context['task_instance']
        ti.xcom_push(key='trigger_edge_device_failed', value=True)
        raise e  # Re-raise the exception to ensure the task is marked as failed


def decide_flow(**context):
    tasks = ['trigger_edge_device', 's3_key_sensor_task', 'copy_s3_file_task']
    states = [TaskInstance(context['dag'].get_task(task), context['execution_date']).state for task in tasks]

    if all(state == 'success' for state in states):
        return 'end_success'
    else:
        return 'end_failure'


def copy_s3_file_callable(source_bucket_key, dest_bucket_key, source_bucket_name, dest_bucket_name, aws_conn_id,
                          **context):
    # Check if the task should fail
    ti = context['task_instance']
    should_fail = context['dag_run'].conf.get('fail_copy_s3_file_task', False) if context.get('dag_run') else False
    if should_fail:
        ti.xcom_push(key='copy_s3_file_failed', value=True)  # Push a value to XCom when the task fails
        raise ValueError("Failing task 'copy_s3_file_task' as per configuration")

    # Execute the S3CopyObjectOperator logic
    operator = S3CopyObjectOperator(
        task_id='copy_s3_file_task',
        source_bucket_key=source_bucket_key,
        dest_bucket_key=dest_bucket_key,
        source_bucket_name=source_bucket_name,
        dest_bucket_name=dest_bucket_name,
        aws_conn_id=aws_conn_id,
    )
    return operator.execute(context=context)



def create_dag(device):
    dag_id = f"report_to_db_dag_{device['name']}"
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'New DAG for Edge Device {device["name"]}',
        schedule_interval='@hourly',
        start_date=datetime(2023, 9, 22),
        catchup=False,
    )

    with dag:
        start_decide = DummyOperator(
            task_id='start_decide',
            dag=dag
        )

        trigger_edge_device_task = PythonOperator(
            task_id='trigger_edge_device',
            python_callable=trigger_edge_device_request,
            op_args=[device['name']],
            execution_timeout=timedelta(seconds=22),
            provide_context=True,
            dag=dag,
            on_failure_callback=handle_failure
        )

        report_trigger_edge_device_task = create_reporting_task_for(trigger_edge_device_task, dag)

        s3_key_sensor_task = PythonOperator(
            task_id='s3_key_sensor_task',
            python_callable=s3_key_sensor_callable,
            op_args=[f'some-prefix/{device["name"]}/{{{{ ds }}}}/task-{{{{ ts_nodash }}}}_{device["name"]}.json',
                     'dank-airflow', 'connect_to_s3_dank_account'],
            provide_context=True,
            dag=dag,
            on_failure_callback=handle_failure  # Added on_failure_callback
        )

        report_s3_key_sensor_task = create_reporting_task_for(s3_key_sensor_task, dag)

        copy_s3_file_task = PythonOperator(
            task_id='copy_s3_file_task',
            python_callable=copy_s3_file_callable,
            op_args=[
                f'some-prefix/{device["name"]}/{{{{ ds }}}}/task-{{{{ ts_nodash }}}}_{device["name"]}.json',
                f'after-copy/{device["name"]}/{{{{ ds }}}}/task-{{{{ ts_nodash }}}}_{device["name"]}.json',
                'dank-airflow',
                'dank-airflow',
                'connect_to_s3_dank_account'
            ],
            provide_context=True,
            dag=dag,
            on_failure_callback=handle_failure  # Added on_failure_callback
        )

        report_copy_s3_file_task = create_reporting_task_for(copy_s3_file_task, dag)

        decide = BranchPythonOperator(
            task_id='decide',
            python_callable=decide_flow,
            provide_context=True,
            dag=dag
        )

        end_success = DummyOperator(task_id='end_success', dag=dag)

        end_failure = DummyOperator(task_id='end_failure', trigger_rule='all_done', dag=dag)

        # Set up the task dependencies.
        start_decide >> trigger_edge_device_task
        trigger_edge_device_task >> [report_trigger_edge_device_task, s3_key_sensor_task]
        s3_key_sensor_task >> [report_s3_key_sensor_task, copy_s3_file_task]
        copy_s3_file_task >> [report_copy_s3_file_task, decide]
        decide >> end_success
        decide >> end_failure

    return dag

for device in EDGE_DEVICES:
    dag = create_dag(device)
    globals()[dag.dag_id] = dag