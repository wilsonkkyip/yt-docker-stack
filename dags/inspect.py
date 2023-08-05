from airflow.operators.python import PythonOperator
from datetime import datetime
from config import dag_config as conf
from airflow import DAG
from operators import InspectOperator

def foo(c):
    print(c)

with DAG(
    dag_id="inspect",
    description="description",
    start_date=datetime(2023,8,1),
    schedule_interval="@daily"
):
    task1 = InspectOperator(
        task_id="task1",
        c={
            "dags_dir": conf.dags_dir,
            "job_id": conf.job_id,
            "input_uri": conf.input_uri,
            "dag_input_uri": conf.dag_input_uri,
            "job_ts": conf.job_ts,
            "output_uri": conf.output_uri,
            "dag_output_uri": conf.dag_output_uri,
            "job_output_uri_ts": conf.job_output_uri_ts,
            "job_output_uri_input_ss": conf.job_output_uri_input_ss,
            "job_output_uri_output_data": conf.job_output_uri_output_data,
        }
    )
    # task1 = PythonOperator(
    #     task_id="task1",
    #     python_callable=foo,
    #     op_kwargs={"c": {
    #         "dags_dir": conf.dags_dir,
    #         "job_id": conf.job_id,
    #         "input_uri": conf.input_uri,
    #         "dag_input_uri": conf.dag_input_uri,
    #         "job_ts": conf.job_ts,
    #         "output_uri": conf.output_uri,
    #         "dag_output_uri": conf.dag_output_uri,
    #         "job_output_uri_ts": conf.job_output_uri_ts,
    #         "job_output_uri_input_ss": conf.job_output_uri_input_ss,
    #         "job_output_uri_output_data": conf.job_output_uri_output_data,
    #     }}
    # )

    task1