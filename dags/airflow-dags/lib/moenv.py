"""AQI DAG 共用：環境部開放資料 API，以及每次執行負責的小時。

每次執行只處理一個固定的小時 T（台北時間，不帶時區，跟 airflow_data 裡其他時間
欄位一樣）。T 由這次執行的 data_interval_end 算出來，所以重跑或 backfill 同一次
執行，抓到的一定是同一個小時，而不是「當下最新的」。

環境部的小時值標的是該小時的結束時間：16:13 就查得到「16:00」的 AQI。訓練時也是
用 T:00 拍的照片配標為 T 的數值（見 jack-lio/VQAweb 的 backend/training）。
"""
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import urllib3

# data.moenv.gov.tw 的憑證鏈在容器裡驗證不過
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API = "https://data.moenv.gov.tw/api/v2"
TAIPEI = ZoneInfo("Asia/Taipei")

# siteid -> (測站名稱, 該縣市的小時值資料集，用來抓氣象)
SITES = {
    "1": ("基隆", "aqx_p_135"),
    "9": ("林口", "aqx_p_137"),
    "42": ("嘉義", "aqx_p_146"),
    "45": ("安南", "aqx_p_149"),
}
STATIONS = [name for name, _ in SITES.values()]


def get(dataset: str, timeout: int = 30, **params) -> list:
    """抓一個環境部資料集。HTTP 錯誤或逾時會直接丟例外，讓 task 失敗、由 Airflow 重試。"""
    query = {"api_key": os.environ["MOENV_API_KEY"], "format": "json", **params}
    res = requests.get(f"{API}/{dataset}", params=query, verify=False, timeout=timeout)
    res.raise_for_status()
    payload = res.json()
    # API 現在直接回傳 list，舊格式是 {"records": [...]}
    return payload.get("records", []) if isinstance(payload, dict) else payload


def target_hour(data_interval_end, lag_hours: int) -> datetime:
    """這次執行負責的小時：data_interval_end 換成台北時間、取整點，再減掉 lag_hours
    （環境部晚多久發布）。回傳不帶時區的台北時間。"""
    # 先轉成時間戳記：Airflow 傳進來的是 pendulum 物件，它的 astimezone() 不接受 ZoneInfo
    end = datetime.fromtimestamp(data_interval_end.timestamp(), TAIPEI)
    end = end.replace(minute=0, second=0, microsecond=0)
    return (end - timedelta(hours=lag_hours)).replace(tzinfo=None)
