from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_date(**kwargs):
    received_date = kwargs['dag_run'].conf['date']
    print(f"Received date Goooooooood  moriinng Vietnam !!!: {received_date}")

with DAG(
    dag_id="target_dag",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=None,  # This DAG is externally triggered, so no schedule
    start_date=datetime(2023, 9, 22),
    catchup=False,
) as dag:
    print_date_task = PythonOperator(
        task_id="print_date_task",
        python_callable=print_date,
        provide_context=True,
    )
