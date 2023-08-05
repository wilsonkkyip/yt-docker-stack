from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pjq import pjq

default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

def _pjq():
    d = {
        "a": {
            "b": "c"
        }
    }
    print("Hello world")
    print(pjq(d, "a.b"))

with DAG(
    dag_id="our_first_dag",
    description="This is our first dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily"
) as dag:
    task1 = BashOperator(
        task_id="first_task",
        bash_command="echo $POSTGRES_USER"
    )

    task2 = PythonOperator(
        task_id="pjq",
        python_callable=_pjq
    )

    task1 >> task2