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


# ✅ DAG ID
DAG_ID = "get_moenv_aqi_info_v2"

# ✅ 抓取資料邏輯
def get_moenv_aqi_info_op(shard_id: int = 0, total_shard: int = 1):
    records = []

    # --- 台灣資料（moenv） ---
    try:
        url = "https://data.moenv.gov.tw/api/v2/aqx_p_432"
        params = {
            # 金鑰由 K8s Secret 以環境變數傳入，不要寫在程式碼裡
            "api_key": os.environ["MOENV_API_KEY"],
            "format": "json",
            "limit": 1000
        }

        res = requests.get(url, params=params, verify=False)
        payload = res.json()
        # API 現在直接回傳 list，舊格式是 {"records": [...]}，兩種都支援
        data = payload.get("records", []) if isinstance(payload, dict) else payload

        target_sites = ["基隆", "林口", "嘉義", "安南"]
        for i, d in enumerate(data):
            if d.get("sitename") not in target_sites:
                continue
            if int(i % total_shard) != int(shard_id):
                continue
            records.append({
                "device_id": str(d.get("sitename")),
                "time": str(d.get("publishtime")),
                "longitude": str(d.get("longitude", "")),
                "latitude": str(d.get("latitude", "")),
                "aqi": str(d.get("aqi", "")),
                "pm2_5": str(d.get("pm2.5", "")),
                "pm10": str(d.get("pm10", "")),
                "o3": str(d.get("o3", "")),
                "co": str(d.get("co", "")),
                "so2": str(d.get("so2", "")),
                "no2": str(d.get("no2", ""))
            })
    except Exception as e:
        print("❌ 台灣資料抓取失敗:", e)

    # --- 印度資料（WAQI） ---
    try:
        TOKEN = os.environ["WAQI_TOKEN"]
        city_list = [
            {"city": "india/eloor/udyogamandal", "device_id": "Eloor Udyogamandal"},
            {"city": "india/thiruvananthapuram/kariavattom", "device_id": "Kariavattom Thiruvananthapuram India"}
        ]

        for idx, item in enumerate(city_list):
            if int(idx % total_shard) != int(shard_id):
                continue

            city = item["city"]
            device_id = item["device_id"]
            url = f"https://api.waqi.info/feed/{city}/?token={TOKEN}"
            response = requests.get(url)
            data = response.json()

            if data["status"] == "ok":
                d = data["data"]
                # 測站沒有回報時間時 time 是空字串，寫入 TIMESTAMP 欄位會失敗
                if not d.get("time", {}).get("s"):
                    print(f"⚠️ WAQI 測站沒有資料時間，略過：{device_id}")
                    continue
                iaqi = d.get("iaqi", {})
                geo = d.get("city", {}).get("geo", [None, None])

                records.append({
                    "device_id": str(device_id),
                    "time": str(d.get("time", {}).get("s", "")),
                    "longitude": str(geo[1]),
                    "latitude": str(geo[0]),
                    "aqi": str(d.get("aqi", "")),
                    "pm2_5": str(iaqi.get("pm25", {}).get("v", "")),
                    "pm10": str(iaqi.get("pm10", {}).get("v", "")),
                    "o3": str(iaqi.get("o3", {}).get("v", "")),
                    "co": str(iaqi.get("co", {}).get("v", "")),
                    "so2": str(iaqi.get("so2", {}).get("v", "")),
                    "no2": str(iaqi.get("no2", {}).get("v", ""))
                })
            else:
                print(f"❌ WAQI 資料抓取失敗：{device_id}")
    except Exception as e:
        print("❌ 印度資料抓取失敗:", e)

    return {"status": "ok", "value": records}


# ✅ 建立 DAG 的函數
def get_aqi_info_dag():
    with DagFactory(
        DAG_ID,
        "airflow",
        start_date=days_ago(1),
        schedule="@hourly",
        catchup=False,
        params={
            "shard_id": Param(0, type="integer"),
            "total_shard": Param(1, type="integer"),
            "connection_id": Param("postgres_default", type="string")
        },
        template_searchpath=["/tmp", os.path.dirname(os.path.realpath(__file__))]
    ).dag() as dag:

        pop = PostgresOp(dag, dag.params["connection_id"])
        shard_id = dag.params["shard_id"]
        total_shard = dag.params["total_shard"]

        # 建立資料表
        createtabletask = pop.create_table("sql/aqi4_info_schema.sql")

        # 刪除 N 天前的舊資料（例如保留 3 天）
        delete_old_task = pop.execute_sql_task(
        sql="""
            DELETE FROM aqi4_info
            WHERE time < NOW() - INTERVAL '3 days';
        """,
        task_id="delete_old_data"
)

        # 抓資料
        gettask = PythonOperator(
            task_id="get_moenv_aqi_info",
            python_callable=get_moenv_aqi_info_op,
            op_kwargs={"shard_id": shard_id, "total_shard": total_shard},
            do_xcom_push=True,
            dag=dag
        )

        # 產生 SQL 檔案
        tmpfile = f"/tmp/aqi_info_bulk_stmt_{shard_id}_{total_shard}.sql"
        generatesql = pop.generate_bulk_insert_stmt(
            table_name="aqi4_info",
            from_task_id="get_moenv_aqi_info",
            out_sql_file=tmpfile,
            update_on_conflict=False,
            on_conflict_key="device_id"
        )

        # 執行 SQL 寫入
        insertdata = pop.bulk_insert_from_stmt(tmpfile)

        # DAG 流程串接
        createtabletask >> delete_old_task >> gettask >> generatesql >> insertdata

    return dag

# ✅ 註冊 DAG 給 Airflow 載入
globals()[DAG_ID] = get_aqi_info_dag()
