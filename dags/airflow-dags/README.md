# airflow-dags

The DAGs that run in the cluster. CI loads every file here in the built
Airflow image (`ci/test_dags.py`) and fails on import errors or a missing DAG.

| File | DAG | Schedule |
|---|---|---|
| `04.aqi-info.py` | `get_moenv_aqi_info_v2` | :30 hourly |
| `07.aqi-image-predict.py` | `aqi_image_predict` | :20 hourly |
| `08.aqi-pipeline-health.py` | `aqi_pipeline_health` | every 6 h |

What each one does is in the repository README.

Shared code (listed in `.airflowignore`, so Airflow doesn't parse it as DAGs):

- `lib/moenv.py`: the MOENV API, the four stations, and `target_hour()`
- `sql/`: the table schemas

## One hour per run

Each run handles one fixed hour, derived from its `data_interval_end` (see
`lib/moenv.py`), and fetches that hour from MOENV's history datasets instead
of whatever is newest. Writes are upserts on the table's primary key, so a
rerun or backfill is safe. To refill hours that were missed:

```bash
kubectl -n airflow exec airflow-scheduler-0 -c scheduler -- \
  airflow dags backfill get_moenv_aqi_info_v2 -s 2026-10-18T00:00:00+08:00 -e 2026-10-29T00:00:00+08:00
```

Missing data makes the task fail and retry; it doesn't succeed with nothing.

The DAGs started from
[footprintai/airflow-dags](https://github.com/footprintai/airflow-dags)
(Apache 2.0, see `LICENSE`).
