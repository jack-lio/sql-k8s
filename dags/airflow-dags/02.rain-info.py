import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import json
import requests

from datetime import datetime
from airflow import DAG
from airflow.models.param import Param
from airflow.models import Variable
#from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator

from dags.factory import DagFactory
from operators.pandas import PandasOp
from operators.postgres import PostgresOp
from lib.requests import debug_requests

DAG_ID = "get_epa_gov_tw_rain_info"

def get_rain_info_op(dag: DAG):
     return PythonOperator(task_id="get_rain_info",
         python_callable=_get_rain_info_op,
         op_kwargs={"apikey": "{{ params.apikey }}"},
         do_xcom_push=True,
         dag=dag)

def _get_rain_info_op(apikey: str):
    # 'Hourly rain info from data.epa.gov.tw'

    url = "https://data.epa.gov.tw/api/v2/acidr_p_01?api_key={}".format(apikey)
    with debug_requests():
        resp = requests.get(url)

    # TODO(hsiny): handle pagination
    # api\/v2\/acidr_p_01?api_key=<API_KEY>&offset=1000
    if resp.status_code != 200:
        return {"status": "failed", "value": ""}

    records = []
    record_keys = {}
    for record in resp.json()["records"]:
        key = key_func(record)
        if key not in record_keys:
            record_keys[key] = True
            records.append(make_record(record))

    # convert custom fields into their types
    #records = []
    #for record in resp.json()["records"]:
    #    record["longitude_numeric"] = float(record["longitude"]) 
    #    record["latitude_numeric"] = float(record["latitude"]) 
    #    record["monitor_date_date"] = datetime.strptime(record["monitor_date"],
    #                                                    '%Y%m%d').date()
    #    if record["result_value"] == "":
    #        record["result_value_numeric"] = 0
    #    else:
    #        record["result_value_numeric"] = float(record["result_value"]) 
    #    records.append(record)
    return {"status": "ok", "value": records}

def key_func(record):
    return "{}-{}-{}-{}".format(record["siteid"], record["monitor_month"],
                                record["monitor_date"], record["item_ename"])

def make_record(record):
    record["time"] = datetime.strptime(record["monitor_date"],
                                       '%Y%m%d').isoformat()
    return record

def get_rain_info_dag():
    with DagFactory("get_epa_gov_tw_rain_info", 
                    "airflow", 
                    datetime(2023, 4, 27),
                    "@daily",
                    params={ "apikey": Param("", type="string"),
                            "connection_id": Param("postgres_default", type="string") },
                    template_searchpath=["/tmp", os.path.dirname(os.path.realpath(__file__))],
                    ).dag() as dag:
        pop = PostgresOp(dag, dag.params["connection_id"])
        pdop = PandasOp(dag)

        getraininfotask = get_rain_info_op(dag)

        createtabletask = pop.create_table("sql/rain_info_schema.sql")

        tmpfile = "/tmp/rain_info_bulk_stmt.sql"

        print("use tmpfile:{}".format(tmpfile))

        generatestms = pop.generate_bulk_insert_stmt("rain_info",
                                                   "get_rain_info",
                                                     tmpfile)
        bulkinserttask = pop.bulk_insert_from_stmt(tmpfile)

        getraininfotask >> createtabletask >> generatestms >>  bulkinserttask 

get_rain_info_dag()
