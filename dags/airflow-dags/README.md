# airflow-dags

The DAGs that run in the cluster. CI loads every file here in the built
Airflow image (`ci/test_dags.py`) and fails on import errors or a missing DAG.

| File | DAG | Schedule |
|---|---|---|
| `04.aqi-info.py` | `get_moenv_aqi_info_v2` | hourly |
| `07.aqi-image-predict.py` | `aqi_image_predict` | :20 hourly |
| `08.aqi-pipeline-health.py` | `aqi_pipeline_health` | every 6 h |

What each one does is in the repository README.

Shared code (listed in `.airflowignore`, so Airflow doesn't parse it as DAGs):

- `dags/factory.py`: `DagFactory`, the common DAG defaults
- `operators/postgres.py`: `PostgresOp`, creates tables and bulk-inserts XCom records
- `lib/requests.py`: request logging for debugging
- `sql/`: the table schemas

The helpers come from
[footprintai/airflow-dags](https://github.com/footprintai/airflow-dags)
(Apache 2.0, see `LICENSE`).
