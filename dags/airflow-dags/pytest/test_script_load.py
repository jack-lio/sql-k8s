import airflow
import pytest
from airflow.models import DagBag

def test_dags_load_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("../02.rain-info.py")
    assert len(dag_bag.import_errors) == 0

