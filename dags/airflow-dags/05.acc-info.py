import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import json
import requests

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from dags.factory import DagFactory
from operators.postgres import PostgresOp
from lib.requests import debug_requests


DAG_ID = "insert_acc05_once"

# ✅ 一次性寫入資料的 Python callable
def insert_acc05_data():
    return {
        "value": [
            {"location": "基隆", "accuracy": 80.6},
            {"location": "林口", "accuracy": 85.7},
            {"location": "嘉義", "accuracy": 80.2},
            {"location": "安南", "accuracy": 83.3},
            {"location": "Udyogamandal", "accuracy": 84.5},
            {"location": "Kariavattom", "accuracy": 86.3}
        ]
    }

def build_dag():
    with DagFactory(
        DAG_ID,
        "airflow",
        start_date=days_ago(1),
        schedule=None,
        catchup=False,
        params={
            "connection_id": Param("postgres_default", type="string")
        },
        template_searchpath=["/tmp", os.path.dirname(os.path.realpath(__file__))]
    ).dag() as dag:

        pop = PostgresOp(dag, dag.params["connection_id"])

        # 建表 task
        createtable = pop.create_table("sql/acc05_result_schema.sql")

        # 資料產生 task
        datagen = PythonOperator(
            task_id="generate_acc05_data",
            python_callable=insert_acc05_data,
            do_xcom_push=True,
            dag=dag
        )

        # 寫入 SQL
        tmpfile = "/tmp/acc05_bulk.sql"
        generatesql = pop.generate_bulk_insert_stmt(
            table_name="acc05_result",
            from_task_id="generate_acc05_data",
            out_sql_file=tmpfile,
            update_on_conflict=True,
            on_conflict_key="location"
        )

        insert = pop.bulk_insert_from_stmt(tmpfile)

        createtable >> datagen >> generatesql >> insert

    return dag

globals()[DAG_ID] = build_dag()