import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import json
import pendulum

from datetime import datetime
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from dags.factory import DagFactory
from operators.postgres import PostgresOp

DAG_ID = "insert_aqi6_from_json"

HERE = os.path.dirname(os.path.realpath(__file__))
FILE_PATH = os.path.join(HERE, "json", "DataCollected.json")
LOCAL_TZ = pendulum.timezone("Asia/Taipei")  # Asia/Taipei (+08)

def parse_json_records():
    with open(FILE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    def fnum(x):
        if x in (None, "", "null", "NaN", "nan"):
            return None
        try:
            return float(x)
        except Exception:
            return None

    out = []
    for r in data.get("records", []):
        t = (r.get("time") or "").strip()
        if not t:
            continue

        # 1) pendulum.parse 能吃 'Z' / 帶 offset 的字串
        # 2) 若字串沒帶時區，就套用 Asia/Taipei（可改成 'UTC' 也行）
        try:
            dt = pendulum.parse(t)
        except Exception:
            continue
        if dt.tzinfo is None:
            # 將「無時區」的時間視為台北時間
            dt = pendulum.instance(dt, tz=LOCAL_TZ)

        # （可選）若你想統一存成 UTC：
        # dt = dt.in_timezone("UTC")

        out.append({
            "device_id": r.get("device_id"),
            "time": dt,  # pendulum.DateTime 是 datetime 的子類，XCom可序列化
            "longitude": fnum(r.get("longitude")),
            "latitude":  fnum(r.get("latitude")),
            "aqi":       fnum(r.get("aqi")),
            "pm2_5":     fnum(r.get("pm2_5")),
            "pm10":      fnum(r.get("pm10")),
            "o3":        fnum(r.get("o3")),
            "co":        fnum(r.get("co")),
            "so2":       fnum(r.get("so2")),
            "no2":       fnum(r.get("no2")),
        })
    return {"value": out}

def build_dag():
    with DagFactory(
        DAG_ID,
        "airflow",
        start_date=days_ago(1),
        schedule=None,
        catchup=False,
        params={"connection_id": Param("postgres_default", type="string")},
        # 和 05 一樣：讓 "sql/xxx.sql" 可以被找到
        template_searchpath=["/tmp", HERE],
    ).dag() as dag:

        pop = PostgresOp(dag, dag.params["connection_id"])

        # 1) 建表（檔案放在 airflow-dags/sql/）
        createtable = pop.create_table("sql/aqi6_info_schema.sql")

        # 2) 解析 JSON
        datagen = PythonOperator(
            task_id="parse_json_records",
            python_callable=parse_json_records,
            do_xcom_push=True,
            dag=dag,
        )

        # 3) 產生 bulk INSERT（UPSERT）
        tmpfile = "/tmp/aqi6_bulk.sql"
        generatesql = pop.generate_bulk_insert_stmt(
            table_name="aqi6_info",
            from_task_id="parse_json_records",
            out_sql_file=tmpfile,
            update_on_conflict=True,
            # 你的 PostgresOp 若要求括號/清單/constraint 名稱，改成其支援的格式
            on_conflict_key="device_id,time",
        )

        # 4) 執行匯入
        insert = pop.bulk_insert_from_stmt(tmpfile)

        createtable >> datagen >> generatesql >> insert

    return dag

globals()[DAG_ID] = build_dag()
