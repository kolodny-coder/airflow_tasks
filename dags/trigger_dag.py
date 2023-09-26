from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="trigger_dag",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 22),
    catchup=False,
) as dag:
    date_to_pass = "2023-09-25"  # This can be dynamically set
    trigger = TriggerDagRunOperator(
        task_id="trigger_target_dag",
        trigger_dag_id="target_dag",  # The ID of the DAG you want to trigger
        conf={"date": date_to_pass}  # Passing the date to the triggered DAG
    )
