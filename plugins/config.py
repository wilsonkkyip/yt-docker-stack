from typing import Text
from os import getenv

class DagConfig:
    @property
    def dags_dir(self) -> Text:
        return f"{getenv('AIRFLOW_HOME')}/dags"

    @property 
    def job_id(self) -> Text:
        return '{{ dag.dag_id }}'
    
    @property
    def input_uri(self) -> Text:
        return f"{getenv('AIRFLOW_HOME')}/dag-inputs"
    
    @property
    def dag_input_uri(self) -> Text:
        return f"{self.input_uri}/{self.job_id}"
    
    @property
    def job_ts(self) -> Text:
        return "{{  ts_nodash  }}"
    
    @property
    def output_uri(self) -> Text:
        return f"{getenv('AIRFLOW_HOME')}/dag-outputs"
    
    @property
    def dag_output_uri(self) -> Text:
        return f"{self.output_uri}/{self.job_id}"
    
    @property
    def job_output_uri_ts(self) -> Text:
        return f"{self.dag_output_uri}/{self.job_ts}"
    
    @property
    def job_output_uri_input_ss(self) -> Text:
        return f"{self.job_output_uri_ts}/input"
    
    @property
    def job_output_uri_output_data(self) -> Text:
        return f"{self.job_output_uri_ts}/output"
    


dag_config = DagConfig()