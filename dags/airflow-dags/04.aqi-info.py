"""每小時把 4 個台灣測站的 AQI 寫進 aqi4_info。

每次執行處理一個固定的小時（見 lib/moenv.py），從環境部的 AQI 歷史資料集
aqx_p_488 抓那一小時的數值，所以漏掉的小時可以用 backfill 補回來。
aqi4_info 是照片推論 (07) 的正確答案，不刪舊資料。
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from lib import moenv

DAG_ID = "get_moenv_aqi_info_v2"
HERE = os.path.dirname(os.path.realpath(__file__))
POSTGRES_CONN_ID = "postgres_default"
AQI_DATASET = "aqx_p_488"  # 空氣品質指標(AQI)(歷史資料)，每小時一筆，可查到 2016 年
LAG_HOURS = 1  # aqx_p_488 在下一小時的 :05 前會有上一小時的資料

# aqi4_info 的欄位 -> aqx_p_488 的欄位
FIELDS = {
    "aqi": "aqi",
    "pm2_5": "pm2.5",
    "pm10": "pm10",
    "o3": "o3",
    "co": "co",
    "so2": "so2",
    "no2": "no2",
    "longitude": "longitude",
    "latitude": "latitude",
}


def load_aqi(data_interval_end):
    """抓這次負責的小時、4 個測站的 AQI，寫進 aqi4_info。缺任何一站就讓 task 失敗重試。"""
    hour = moenv.target_hour(data_interval_end, LAG_HOURS)
    print("小時:", hour)

    rows, missing = [], []
    for station in moenv.STATIONS:
        records = moenv.get(AQI_DATASET, limit=5,
                            filters=f"sitename,EQ,{station}|datacreationdate,EQ,{hour:%Y-%m-%d %H:%M}")
        if not records:
            missing.append(station)
            continue
        r = records[0]
        rows.append((station, hour, *(str(r.get(src, "")) for src in FIELDS.values())))
        print(f"  {station}: AQI {r.get('aqi')}")

    if rows:
        cols = ", ".join(FIELDS)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in FIELDS)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO aqi4_info (device_id, time, {cols}) "
                f"VALUES (%s, %s, {', '.join(['%s'] * len(FIELDS))}) "
                f"ON CONFLICT (device_id, time) DO UPDATE SET {updates}",
                rows,
            )
        print(f"寫入 {len(rows)} 筆")

    # 先寫入抓到的，再失敗：重試時只會補上缺的那幾站
    if missing:
        raise ValueError(f"{hour} 還沒有這些測站的 AQI：{missing}")


with DAG(
    DAG_ID,
    schedule="30 * * * *",
    start_date=datetime(2026, 10, 18),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=15),
    },
    template_searchpath=[HERE],
    tags=["aqi"],
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/aqi4_info_schema.sql",
    )
    load = PythonOperator(task_id="load_aqi", python_callable=load_aqi)

    create_table >> load
