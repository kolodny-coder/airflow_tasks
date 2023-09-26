from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# List of edge devices
EDGE_DEVICES = [
    {"id": "device_001", "other_param": "value1"},
    {"id": "device_002", "other_param": "value2"},
    # ... add all your devices here
]

def print_edge_device_name(device_id, **kwargs):
    """
    Python callable for the PythonOperator.
    """
    print(f"Processing data for edge device: {device_id}")

def create_dag(device):
    """
    This function returns a DAG object for a given device.
    """
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
        start_task = DummyOperator(
            task_id='start',
        )

        print_device_task = PythonOperator(
            task_id='print_device_name',
            python_callable=print_edge_device_name,
            op_args=[device['id']],
            provide_context=True,
        )

        end_task = DummyOperator(
            task_id='end',
        )

        start_task >> print_device_task >> end_task

    return dag

# Dynamically generate a DAG for each device
for device in EDGE_DEVICES:
    dag = create_dag(device)
    globals()[dag.dag_id] = dag  # This makes the DAG discoverable by Airflow