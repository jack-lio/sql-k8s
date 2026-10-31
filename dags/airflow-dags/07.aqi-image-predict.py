import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import io
import re
import zipfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import urllib3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# data.moenv.gov.tw 的憑證鏈在容器裡驗證不過，04.aqi-info.py 也是 verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DAG_ID = "aqi_image_predict"
HERE = os.path.dirname(os.path.realpath(__file__))

MOENV_API = "https://data.moenv.gov.tw/api/v2"
IMAGE_DATASET = "aqx_p_01"  # 空氣品質監測即時影像資料：每 10 分鐘一個 zip，內含各測站最新照片
BUCKET = "aqi-images"
S3_CONN_ID = "minio_aqi"            # AIRFLOW_CONN_MINIO_AQI，來自 airflow-dag-secrets
POSTGRES_CONN_ID = "postgres_default"
PREDICT_URL = os.environ.get("AQI_PREDICT_URL", "http://vqaweb.default.svc/api/predict")
MODEL_NAME = "A"  # Taiwan_model

# 跟 04.aqi-info.py 同樣的 4 個測站：siteid -> (測站名稱, 該縣市的小時值資料集)
SITES = {
    "1": ("基隆", "aqx_p_135"),
    "9": ("林口", "aqx_p_137"),
    "42": ("嘉義", "aqx_p_146"),
    "45": ("安南", "aqx_p_149"),
}
# 模型的 5 個氣象輸入 -> 環境部的監測項目名稱
WEATHER_ITEMS = {
    "rh": "RH",
    "rainfall": "RAINFALL",
    "temperature": "AMB_TEMP",
    "wd_hr": "WD_HR",
    "ws_hr": "WS_HR",
}
# 環境部每 10 分鐘發布一包照片；最新一包比這更舊，就當作沒拿到新資料
MAX_ZIP_AGE = timedelta(minutes=40)
IMAGE_NAME = re.compile(r"^(\d{3})-(\d{12})\.jpg$")  # e.g. 001-202610181000.jpg


def _moenv(dataset: str, **params):
    query = {"api_key": os.environ["MOENV_API_KEY"], "format": "json", **params}
    res = requests.get(f"{MOENV_API}/{dataset}", params=query, verify=False, timeout=60)
    res.raise_for_status()
    payload = res.json()
    return payload.get("records", []) if isinstance(payload, dict) else payload


def fetch_images():
    """下載最新的測站照片 zip，把 4 個測站的照片存進 MinIO。"""
    # 沒指定排序時，API 不保證新的在前：10/31 00:20–04:20 一直拿到 23:55 那包舊 zip
    files = _moenv(IMAGE_DATASET, limit=5, sort="upload_date desc")
    if not files:
        raise ValueError(f"{IMAGE_DATASET} 沒有回傳任何檔案")
    latest = max(files, key=lambda f: f["upload_date"])
    print("zip:", latest["filename"], latest["upload_date"])
    # upload_date 是台灣時間，不帶時區
    age = datetime.now(ZoneInfo("Asia/Taipei")).replace(tzinfo=None) \
        - datetime.strptime(latest["upload_date"], "%Y%m%dT%H%M%S")
    if age > MAX_ZIP_AGE:
        # 失敗後會重試；不失敗的話，舊照片會被 ON CONFLICT 略過，看起來像成功但沒有新資料
        raise ValueError(f"最新的 zip 已經是 {age} 前的，API 可能回傳了舊資料")

    res = requests.get(latest["url"], timeout=300)
    res.raise_for_status()

    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    images = {}
    with zipfile.ZipFile(io.BytesIO(res.content)) as zf:
        for name in zf.namelist():
            m = IMAGE_NAME.match(os.path.basename(name))
            if not m:
                continue
            site_id = str(int(m.group(1)))
            if site_id not in SITES:
                continue
            taken = datetime.strptime(m.group(2), "%Y%m%d%H%M")
            key = f"stations/{site_id}/{taken:%Y/%m/%d/%H%M}.jpg"
            s3.load_bytes(zf.read(name), key=key, bucket_name=BUCKET, replace=True)
            images[site_id] = {"image_key": key, "image_time": taken.isoformat()}
            print(f"  {SITES[site_id][0]}: {name} -> s3://{BUCKET}/{key}")

    missing = sorted(set(SITES) - set(images))
    if missing:
        print("zip 裡沒有這些測站的照片:", [SITES[s][0] for s in missing])
    if not images:
        raise ValueError("zip 裡找不到任何目標測站的照片")
    return images


def fetch_weather():
    """從各縣市的小時值資料集，取每個測站最新一小時的 5 個氣象值。"""
    weather = {}
    for site_id, (sitename, dataset) in SITES.items():
        try:
            rows = _moenv(dataset, filters=f"SiteName,EQ,{sitename}",
                          sort="monitordate desc", limit=200)
        except requests.RequestException as e:
            print(f"  {sitename}: 抓取失敗 {e}")
            continue
        by_hour = {}
        for r in rows:
            by_hour.setdefault(r["monitordate"], {})[r["itemengname"]] = r["concentration"]
        # 最新且 5 個值都是數字的那一小時
        for hour in sorted(by_hour, reverse=True):
            values = {}
            for field, item in WEATHER_ITEMS.items():
                try:
                    values[field] = float(by_hour[hour][item])
                except (KeyError, TypeError, ValueError):
                    break
            else:
                weather[site_id] = {"weather_time": hour, **values}
                print(f"  {sitename} {hour}: {values}")
                break
        else:
            print(f"  {sitename}: 找不到完整的氣象資料")
    if not weather:
        raise ValueError("所有測站都抓不到氣象資料")
    return weather


def predict(ti):
    """用 MinIO 裡的照片 + 氣象值呼叫推論服務，結果寫進 aqi_prediction。"""
    images = ti.xcom_pull(task_ids="fetch_images")
    weather = ti.xcom_pull(task_ids="fetch_weather")
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)

    rows = []
    for site_id in sorted(set(images) & set(weather)):
        sitename = SITES[site_id][0]
        img, w = images[site_id], weather[site_id]
        body = s3.get_key(img["image_key"], bucket_name=BUCKET).get()["Body"].read()
        form = {k: str(w[k]) for k in WEATHER_ITEMS}
        form["model_name"] = MODEL_NAME
        res = requests.post(PREDICT_URL, data=form,
                            files={"image": (os.path.basename(img["image_key"]), body, "image/jpeg")},
                            timeout=120)
        res.raise_for_status()
        out = res.json()
        if "error" in out:
            # 後端出錯時也回 200，錯誤放在 error 欄位
            print(f"  {sitename}: 推論失敗 {out['error']}")
            continue
        print(f"  {sitename}: {out['AQI']} (class {out['pred_class']}, {out['regression_value']}, "
              f"{out['model_version']})")
        rows.append((site_id, sitename, img["image_time"], img["image_key"], w["weather_time"],
                     w["rh"], w["rainfall"], w["temperature"], w["wd_hr"], w["ws_hr"],
                     MODEL_NAME, out["model_version"], out["pred_class"], out["AQI"],
                     out["regression_value"]))

    if not rows:
        raise ValueError("沒有任何測站推論成功")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO aqi_prediction (site_id, sitename, image_time, image_key, weather_time,
                rh, rainfall, temperature, wd_hr, ws_hr,
                model_name, model_version, pred_class, pred_label, regression_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (site_id, image_time, model_name) DO NOTHING
            """,
            rows,
        )
    print(f"寫入 {len(rows)} 筆預測")
    return len(rows)


with DAG(
    DAG_ID,
    # DAG 04 在整點抓 AQI；:20 時照片和上一小時的氣象值都已經發布
    schedule="20 * * * *",
    start_date=datetime(2026, 10, 18),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath=[HERE],
    tags=["aqi"],
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/aqi_prediction_schema.sql",
    )
    images_task = PythonOperator(task_id="fetch_images", python_callable=fetch_images)
    weather_task = PythonOperator(task_id="fetch_weather", python_callable=fetch_weather)
    predict_task = PythonOperator(task_id="predict", python_callable=predict)

    create_table >> [images_task, weather_task] >> predict_task
