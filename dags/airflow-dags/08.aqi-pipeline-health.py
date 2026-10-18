"""每 6 小時檢查整條 AQI 流程，並請 Claude 判讀結果（AI workflow）。

步驟是固定的，只有 ai_review 這一步交給 AI：
  collect_signals  收集各元件的狀態，並用固定規則先判斷一次（不需要 AI）
  ai_review        把收集到的資料交給 Claude，產生結構化的健康報告
                   （沒有 ANTHROPIC_API_KEY 時跳過，報告只用規則判斷的結果）
  store_report     報告寫進 Postgres（Grafana 顯示）和 MinIO（Markdown）
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from airflow import DAG
from airflow.models import DagModel, DagRun
from airflow.operators.python import ExternalPythonOperator, PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.session import create_session

DAG_ID = "aqi_pipeline_health"
HERE = os.path.dirname(os.path.realpath(__file__))
TAIPEI = ZoneInfo("Asia/Taipei")

POSTGRES_CONN_ID = "postgres_default"
S3_CONN_ID = "minio_aqi"
BUCKET = "aqi-images"
PREDICT_HEALTH_URL = "http://vqaweb.default.svc/api/health"
PROMETHEUS_URL = "http://prometheus.monitoring.svc:9090"
SDK_PYTHON = "/home/airflow/anthropic-venv/bin/python"  # 見 Dockerfile

WATCHED_DAGS = ["get_moenv_aqi_info_v2", "aqi_image_predict"]
TW_STATIONS = ["基隆", "林口", "嘉義", "安南"]
STALE_HOURS = 3


def _hours_since(ts, now):
    return None if ts is None else round((now - ts).total_seconds() / 3600, 2)


def collect_signals():
    """收集各元件的狀態；每個來源各自 try，一個壞掉不影響其他檢查。"""
    now = datetime.now(TAIPEI)
    now_local = now.replace(tzinfo=None)  # aqi4_info / aqi_prediction 存的是台北時間，沒有時區
    signals = {"checked_at": now.isoformat(), "errors": {}}
    findings = []

    def finding(component, severity, text):
        findings.append({"component": component, "severity": severity, "finding": text})

    # --- Airflow：各 DAG 過去 24 小時的執行結果
    try:
        dags = {}
        since = datetime.now(ZoneInfo("UTC")) - timedelta(hours=24)
        with create_session() as session:
            for dag_id in WATCHED_DAGS:
                runs = (session.query(DagRun)
                        .filter(DagRun.dag_id == dag_id, DagRun.execution_date >= since).all())
                model = session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()
                states = {}
                for r in runs:
                    states[str(r.state)] = states.get(str(r.state), 0) + 1
                successes = [r.end_date for r in runs if str(r.state) == "success" and r.end_date]
                last_ok = max(successes).astimezone(TAIPEI) if successes else None
                dags[dag_id] = {
                    "paused": bool(model.is_paused) if model else None,
                    "runs_24h": states,
                    "last_success": last_ok.isoformat() if last_ok else None,
                    "hours_since_success": _hours_since(last_ok, now),
                }
                if model is None:
                    finding("airflow", "critical", f"找不到 DAG {dag_id}")
                elif model.is_paused:
                    finding("airflow", "critical", f"DAG {dag_id} 被暫停了，不會抓新資料")
                if states.get("failed"):
                    finding("airflow", "warning", f"DAG {dag_id} 過去 24 小時失敗 {states['failed']} 次")
                if last_ok is None or (now - last_ok) > timedelta(hours=STALE_HOURS):
                    finding("airflow", "critical", f"DAG {dag_id} 超過 {STALE_HOURS} 小時沒有成功執行")
        signals["airflow"] = dags
    except Exception as e:  # noqa: BLE001 - 收集失敗也要記錄下來交給報告
        signals["errors"]["airflow"] = repr(e)
        finding("airflow", "warning", f"無法讀取 Airflow 執行紀錄：{e!r}")

    # --- Postgres：AQI 資料、照片推論、資料庫大小
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records(
            "SELECT device_id, max(time), count(*) FROM aqi4_info "
            "WHERE device_id = ANY(%s) GROUP BY device_id", parameters=(TW_STATIONS,))
        aqi = {d: {"latest": t.isoformat(), "hours_old": _hours_since(t, now_local), "rows": n}
               for d, t, n in rows}
        for s in TW_STATIONS:
            if s not in aqi:
                finding("aqi_data", "critical", f"aqi4_info 沒有測站 {s} 的資料")
            elif aqi[s]["hours_old"] > STALE_HOURS:
                finding("aqi_data", "warning", f"測站 {s} 的 AQI 資料已經 {aqi[s]['hours_old']} 小時沒更新")
        signals["aqi_data"] = aqi

        rows = hook.get_records(
            "SELECT sitename, max(image_time), max(weather_time), "
            "count(*) FILTER (WHERE image_time > %s) FROM aqi_prediction GROUP BY sitename",
            parameters=(now_local - timedelta(hours=24),))
        pred = {s: {"latest_image": i.isoformat(), "image_hours_old": _hours_since(i, now_local),
                    "latest_weather": w.isoformat(), "weather_hours_old": _hours_since(w, now_local),
                    "predictions_24h": n}
                for s, i, w, n in rows}
        for s in TW_STATIONS:
            if s not in pred:
                finding("inference", "warning", f"測站 {s} 沒有任何照片推論結果")
            elif pred[s]["image_hours_old"] > STALE_HOURS:
                finding("inference", "warning",
                        f"測站 {s} 最新的照片推論已經是 {pred[s]['image_hours_old']} 小時前")
        signals["predictions"] = pred

        # 預測等級 vs 照片拍攝那一小時環境部公布的 AQI 等級
        acc = hook.get_first(
            """
            SELECT count(*),
                   count(*) FILTER (WHERE p.pred_class = CASE
                       WHEN a.aqi::float <= 50 THEN 0 WHEN a.aqi::float <= 100 THEN 1
                       WHEN a.aqi::float <= 150 THEN 2 WHEN a.aqi::float <= 200 THEN 3
                       WHEN a.aqi::float <= 300 THEN 4 ELSE 5 END)
            FROM aqi_prediction p
            JOIN aqi4_info a ON a.device_id = p.sitename AND a.time = date_trunc('hour', p.image_time)
            WHERE a.aqi ~ '^[0-9]+(\\.[0-9]+)?$' AND p.image_time > %s
              AND p.model_version <> 'legacy'
            """, parameters=(now_local - timedelta(hours=24),))
        signals["prediction_accuracy_24h"] = {
            "compared": acc[0], "matched": acc[1],
            "accuracy": round(acc[1] / acc[0], 3) if acc[0] else None}

        signals["database_bytes"] = hook.get_first("SELECT pg_database_size(current_database())")[0]
    except Exception as e:  # noqa: BLE001
        signals["errors"]["postgres"] = repr(e)
        finding("postgres", "critical", f"無法查詢 airflow_data：{e!r}")

    # --- MinIO：照片數量與大小
    try:
        s3 = S3Hook(aws_conn_id=S3_CONN_ID).get_conn()
        total = size = recent = 0
        latest = None
        cutoff = datetime.now(ZoneInfo("UTC")) - timedelta(hours=24)
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="stations/"):
            for obj in page.get("Contents", []):
                total += 1
                size += obj["Size"]
                recent += obj["LastModified"] >= cutoff
                latest = max(latest, obj["LastModified"]) if latest else obj["LastModified"]
        signals["minio"] = {
            "images": total, "bytes": size, "images_24h": recent,
            "bucket_quota_bytes": 5 * 1024 ** 3,
            "latest_upload": latest.astimezone(TAIPEI).isoformat() if latest else None,
        }
        if latest is None or datetime.now(ZoneInfo("UTC")) - latest > timedelta(hours=STALE_HOURS):
            finding("minio", "warning", f"超過 {STALE_HOURS} 小時沒有新照片存進 MinIO")
        if size > 0.9 * 5 * 1024 ** 3:
            finding("minio", "warning", "aqi-images 已使用超過 90% 的 5GiB 上限")
    except Exception as e:  # noqa: BLE001
        signals["errors"]["minio"] = repr(e)
        finding("minio", "critical", f"無法連線 MinIO：{e!r}")

    # --- 推論服務
    try:
        res = requests.get(PREDICT_HEALTH_URL, timeout=10)
        signals["inference_service"] = {"http_status": res.status_code, "body": res.text[:200]}
        if res.status_code != 200:
            finding("inference", "critical", f"推論服務 /api/health 回傳 {res.status_code}")
    except requests.RequestException as e:
        signals["errors"]["inference_service"] = repr(e)
        finding("inference", "critical", f"推論服務連不上：{e!r}")

    # --- Prometheus 正在觸發的告警
    try:
        alerts = requests.get(f"{PROMETHEUS_URL}/api/v1/alerts", timeout=10).json()["data"]["alerts"]
        firing = [{"alert": a["labels"].get("alertname"), "severity": a["labels"].get("severity"),
                   "summary": a.get("annotations", {}).get("summary"), "since": a.get("activeAt")}
                  for a in alerts if a.get("state") == "firing"]
        signals["firing_alerts"] = firing
        for a in firing:
            finding("monitoring", "warning", f"告警觸發中：{a['alert']} - {a['summary']}")
    except (requests.RequestException, KeyError, ValueError) as e:
        signals["errors"]["prometheus"] = repr(e)
        finding("monitoring", "warning", f"無法讀取 Prometheus 告警：{e!r}")

    signals["rule_findings"] = findings
    for f in findings:
        print(f"[{f['severity']}] {f['component']}: {f['finding']}")
    return signals


def ai_review(signals):
    """在獨立的 venv 裡執行（ExternalPythonOperator），所以所有 import 都寫在函式裡。"""
    import json
    import os

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY 沒有設定，跳過 AI 分析")
        return {"status": "skipped", "reason": "ANTHROPIC_API_KEY not set"}

    import anthropic

    system = (
        "你是一套小型資料管線的 SRE 值班助手。請根據提供的監控資料，判斷整條流程是否健康，"
        "並用繁體中文寫出給工程師看的報告。\n\n"
        "流程（全部跑在一台 minikube 上）：\n"
        "1. Airflow DAG get_moenv_aqi_info_v2：每小時整點從環境部抓 4 個台灣測站（基隆、林口、嘉義、安南）"
        "的 AQI，寫進 Postgres 的 aqi4_info（只保留 3 天）。\n"
        "2. Airflow DAG aqi_image_predict：每小時 :20 抓這 4 個測站的監測站照片存進 MinIO，"
        "抓上一小時的氣象值，呼叫推論服務 (vqaweb) 預測 AQI 等級，寫進 aqi_prediction。\n"
        "3. Prometheus + Grafana 監控以上元件。\n\n"
        "判讀原則：\n"
        "- rule_findings 是固定規則的判斷結果，請以 signals 的原始數據為準再確認一次，"
        "並找出規則沒涵蓋到的問題（例如多個症狀其實是同一個原因）。\n"
        "- 環境部的資料本來就有約 1 小時的發布延遲；氣象值比照片晚 1～2 小時是正常的。\n"
        "- 已知限制：推論模型的影像特徵擷取器目前是隨機初始化的，準確率低是預期中的，"
        "請寫在 model_quality_note，不要因此把整體狀態判為 failing。\n"
        "- overall_status：healthy = 資料準時、沒有失敗；degraded = 有延遲或部分失敗但資料仍在進來；"
        "failing = 資料已經停止進來，或關鍵元件連不上。\n"
        "- 每個 issue 的 evidence 要引用具體數值，suggested_action 要寫可以直接執行的檢查步驟或指令。"
    )
    schema = {
        "type": "object",
        "properties": {
            "overall_status": {"type": "string", "enum": ["healthy", "degraded", "failing"]},
            "summary": {"type": "string"},
            "issues": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "component": {"type": "string", "enum": [
                            "airflow", "aqi_data", "images", "weather", "inference",
                            "minio", "postgres", "monitoring", "other"]},
                        "severity": {"type": "string", "enum": ["info", "warning", "critical"]},
                        "finding": {"type": "string"},
                        "evidence": {"type": "string"},
                        "suggested_action": {"type": "string"},
                    },
                    "required": ["component", "severity", "finding", "evidence", "suggested_action"],
                    "additionalProperties": False,
                },
            },
            "model_quality_note": {"type": "string"},
        },
        "required": ["overall_status", "summary", "issues", "model_quality_note"],
        "additionalProperties": False,
    }

    client = anthropic.Anthropic()
    try:
        response = client.beta.messages.create(
            model="claude-opus-5-5",
            max_tokens=16000,
            # 安全分類器拒絕時，由 Anthropic 依拒絕類別自動改用合適的備援模型重跑
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            output_config={
                "effort": "medium",
                "format": {"type": "json_schema", "schema": schema},
            },
            system=system,
            messages=[{"role": "user", "content": json.dumps(signals, ensure_ascii=False, sort_keys=True)}],
        )
    except anthropic.AuthenticationError as e:
        return {"status": "error", "error": f"API key 無效：{e.message}"}
    except anthropic.RateLimitError as e:
        return {"status": "error", "error": f"超過速率限制：{e.message}"}
    except anthropic.APIStatusError as e:
        return {"status": "error", "error": f"API 錯誤 {e.status_code}：{e.message}"}
    except anthropic.APIConnectionError as e:
        return {"status": "error", "error": f"連不上 Claude API：{e!r}"}

    usage = {"input_tokens": response.usage.input_tokens, "output_tokens": response.usage.output_tokens}
    if response.stop_reason == "refusal":
        category = response.stop_details.category if response.stop_details else None
        return {"status": "refused", "error": f"refusal ({category})", "model": response.model, **usage}
    if response.stop_reason == "max_tokens":
        return {"status": "error", "error": "回應超過 max_tokens", "model": response.model, **usage}

    text = next(b.text for b in response.content if b.type == "text")
    print(f"request_id={response._request_id} model={response.model} usage={usage}")
    return {"status": "ok", "model": response.model, "report": json.loads(text), **usage}


def _status_from_rules(findings):
    severities = {f["severity"] for f in findings}
    if "critical" in severities:
        return "failing"
    return "degraded" if "warning" in severities else "healthy"


def _markdown(checked_at, status, source, summary, issues, note, ai):
    lines = [f"# AQI 流程健康報告 {checked_at}", "", f"- 整體狀態：**{status}**（判斷來源：{source}）",
             f"- AI 分析：{ai.get('status')} {ai.get('model') or ai.get('reason') or ai.get('error') or ''}",
             "", "## 摘要", "", summary, "", "## 問題", ""]
    if not issues:
        lines.append("沒有發現問題。")
    for i in issues:
        lines += [f"### [{i['severity']}] {i['component']}：{i['finding']}", ""]
        if i.get("evidence"):
            lines += [f"- 證據：{i['evidence']}"]
        if i.get("suggested_action"):
            lines += [f"- 建議：{i['suggested_action']}"]
        lines.append("")
    if note:
        lines += ["## 模型品質", "", note, ""]
    return "\n".join(lines)


def store_report(ti):
    signals = ti.xcom_pull(task_ids="collect_signals")
    ai = ti.xcom_pull(task_ids="ai_review") or {"status": "error", "error": "ai_review 沒有回傳結果"}
    findings = signals["rule_findings"]

    if ai.get("status") == "ok":
        report = ai["report"]
        status, source = report["overall_status"], "ai"
        summary, issues, note = report["summary"], report["issues"], report["model_quality_note"]
    else:
        status, source = _status_from_rules(findings), "rules"
        summary = f"規則檢查發現 {len(findings)} 個問題。" if findings else "規則檢查沒有發現問題。"
        issues = [{**f, "evidence": "", "suggested_action": ""} for f in findings]
        note = ""

    checked_at = datetime.fromisoformat(signals["checked_at"])
    key = f"reports/{checked_at:%Y/%m/%d/%H%M}.md"
    S3Hook(aws_conn_id=S3_CONN_ID).load_string(
        _markdown(signals["checked_at"], status, source, summary, issues, note, ai),
        key=key, bucket_name=BUCKET, replace=True)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_health_report (checked_at, overall_status, source, summary, issues,
                rule_findings, signals, ai_status, ai_model, input_tokens, output_tokens, report_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (checked_at, status, source, summary, json.dumps(issues, ensure_ascii=False),
             json.dumps(findings, ensure_ascii=False), json.dumps(signals, ensure_ascii=False),
             ai.get("status"), ai.get("model"), ai.get("input_tokens"), ai.get("output_tokens"), key),
        )
    print(f"overall={status} source={source} issues={len(issues)} report=s3://{BUCKET}/{key}")
    return status


with DAG(
    DAG_ID,
    schedule="45 */6 * * *",
    start_date=datetime(2026, 10, 18),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)},
    template_searchpath=[HERE],
    # 讓 {{ ti.xcom_pull(...) }} 傳給 ai_review 的是 dict，不是字串
    render_template_as_native_obj=True,
    tags=["aqi", "ai"],
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/pipeline_health_schema.sql",
    )
    collect = PythonOperator(task_id="collect_signals", python_callable=collect_signals)
    review = ExternalPythonOperator(
        task_id="ai_review",
        python=SDK_PYTHON,
        python_callable=ai_review,
        op_kwargs={"signals": "{{ ti.xcom_pull(task_ids='collect_signals') }}"},
        expect_airflow=False,
        expect_pendulum=False,
    )
    store = PythonOperator(task_id="store_report", python_callable=store_report)

    create_table >> collect >> review >> store
