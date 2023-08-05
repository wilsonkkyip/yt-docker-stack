import os
from airflow import DAG
from datetime import datetime
from operators import (
    MakeDirsOperator, CopyFilesOperator, DownloadChannelsOperator
)
from config import dag_config as conf

with DAG(
    dag_id="download_channels",
    start_date=datetime(2023,8,1,0,0,0),
    schedule="@once",
    concurrency=1
) as dag:
    # Make job output
    make_dirs = MakeDirsOperator(
        task_id="make_dirs",
        dirs=[
            conf.job_output_uri_ts, 
            conf.job_output_uri_input_ss,
            conf.job_output_uri_output_data
        ]
    )

    # Snapshot input
    snapshot_input = CopyFilesOperator(
        task_id="snapshot_input",
        src_path=conf.dag_input_uri,
        des_path=conf.job_output_uri_input_ss,
        include_files=["input.txt"]
    )

    # Request API
    download_channels = DownloadChannelsOperator(
        task_id="download_channels",
        # ids="{{ ti.xcom_pull(task_ids='read_input') }}",
        api_key=os.getenv("GAPI_KEY"),
        output_dir=conf.job_output_uri_output_data
    )

    make_dirs >> snapshot_input >> download_channels
    
