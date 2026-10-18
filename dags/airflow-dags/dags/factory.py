from contextlib import contextmanager
from pathlib import Path

from airflow import DAG

class DagFactory:

    def __init__(self, 
                 dag_id: str, 
                 owner: str, 
                 start_date: str, 
                 schedule:str,
                 params: dict,
                 template_searchpath: str,
                 catchup: bool = False,
                 ):
        self.dag_id = dag_id
        self.schedule = schedule
        self.default_args = {
            'owner': owner,
            'start_date': start_date
        }
        self.params = params
        self.template_searchpath = template_searchpath
        self.catchup = catchup

    @contextmanager
    def dag(self):
        with DAG(self.dag_id,
                 default_args=self.default_args,
                 schedule_interval=self.schedule,
                 params=self.params,
                 template_searchpath=self.template_searchpath,
                 catchup=self.catchup,
                 ) as dag:

            # register globally
            globals()[self.dag_id] = dag

            yield dag
