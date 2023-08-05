from typing import List, Text, Dict, Any
import logging
from logging import Logger
from pathlib import Path
import shutil
from shutil import rmtree
from airflow.models import BaseOperator
from glob import glob
from youtube import download_channels
from math import log10, ceil, floor
import os

class LoggingMixin:
    @property
    def log(self) -> Logger:
        if not hasattr(self, '_log'):
            self._log = logging.getLogger(
                'airflow.task.' + self.__class__.__module__ + '.' + self.__class__.__name__
            )
        return self._log

# class ReadFileOperator(BaseOperator):
#     template_fields = ['path']
#     def __init__(
#         self,
#         path: Text,
#         *args: Any,
#         **kwargs: Any
#     ) -> Text:
#         super().__init__(*args, **kwargs)

#         self.path = path
    
#     def execute(self, context):
#         self.log.info(f"Reading file: {self.path}")
#         with open(self.path, "r") as f:
#             content = f.read()
#         return content
        

class DownloadChannelsOperator(BaseOperator):
    template_fields = ['output_dir']
    def __init__(
        self,
        api_key: Text,
        output_dir: Text,
        *args: Any,
        **kwargs: Any
    ):
        super().__init__(*args, **kwargs)

        self.api_key = api_key
        self.output_dir = output_dir

    def execute(self, context): 
        n = 50  # Maximum number of channels per request
        self.log.info("Reading input file")
        airflow_home = os.getenv("AIRFLOW_HOME")
        dag_id = context["dag"].dag_id
        ts = context["ts_nodash"]
        input_fname = f"{airflow_home}/dag-outputs/{dag_id}/{ts}/input/input.txt"
        with open(input_fname, "r") as f:
            ids = [x.replace("\n", "") for x in f.readlines()]
        ids = [
            ids[(n * i):(n * (i + 1))] for i in range(ceil(len(ids) / n))
        ]
        d = floor(log10(len(ids)) + 1)
        for i, id in enumerate(ids):
            b = str(i).zfill(d)
            self.log.info(f"Downloading batch{b}: {id}")
            id = ",".join(id)
            fname = f"{self.output_dir}/batch{b}.json"
            download_channels(id, self.api_key, fname)
        
        

class InspectOperator(BaseOperator):
    template_fields = ['c']
    def __init__(
        self,
        c,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.c = c
    
    def execute(self, context):
        print(context)


class CopyFilesOperator(BaseOperator):
    template_fields = ['src_path', 'des_path', 'include_files']
    def __init__(
        self,
        src_path: Text,
        des_path: Text,
        include_files: List[Text],
        *args: Any,
        **kwargs: Any
    ) -> None: 
        super().__init__(*args, **kwargs)

        self.src_path = src_path
        self.des_path = des_path
        self.include_files = include_files
    
    def execute(self, context):
        targeted_files = [
            glob(f"{self.src_path}/{x}") for x in self.include_files
        ]
        self.log.info(f"Files to be copied:")
        for files in targeted_files:
            for file in files:
                self.log.info(f"{file}")

        for files in targeted_files:
            for file in files:
                self.log.info(f"Copying file: '{file}' to '{self.des_path}'")
                shutil.copy(file, f"{self.des_path}/")

class MakeDirsOperator(BaseOperator):
    template_fields = ['dirs']

    def __init__(
        self,
        dirs: List[Text],
        *args: Any,
        overwrite: bool = True,
        **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)

        self.dirs = dirs
        self.overwrite = overwrite
        self.log.info(f"DIRS - {self.dirs}")
    
    def execute(self, context) -> Any:
        for path in self.dirs:
            path = Path(path)
            self.log.info(f"Creating directory: {path.resolve()}")
            if path.exists():
                self.log.info(f"Directory exists: {path.resolve()}")
                if self.overwrite:
                    self.log.info(
                        f"Removing existing path: {path.resolve()}"
                    )
                    if path.is_dir():
                        rmtree(path.resolve())
                    else:
                        path.unlink()
                    self.log.info(
                        f"Creating new directory: {path.resolve()}"
                    )
                    path.mkdir()
                else:
                    self.log.info(
                        f"Skip creating directory: {path.resolve()}"
                    )
            else:
                path.mkdir()




