"""DAG integrity check, run inside the built Airflow image by CI.

Fails when any DAG file has an import error, or when a DAG the pipeline
depends on is missing or has no tasks.
"""
import sys

from airflow.models import DagBag

REQUIRED = {
    "get_moenv_aqi_info_v2": 1,   # 04: hourly AQI into Postgres
    "aqi_image_predict": 4,       # 07: station images -> MinIO -> vqaweb
    "aqi_pipeline_health": 3,     # 08: health check + AI review
}

bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
failed = False

for path, error in bag.import_errors.items():
    print(f"IMPORT ERROR {path}\n{error}")
    failed = True

for dag_id, min_tasks in REQUIRED.items():
    dag = bag.dags.get(dag_id)
    if dag is None:
        print(f"MISSING DAG {dag_id}")
        failed = True
    elif len(dag.tasks) < min_tasks:
        print(f"DAG {dag_id} has {len(dag.tasks)} tasks, expected at least {min_tasks}")
        failed = True

print(f"{len(bag.dags)} DAGs loaded: {', '.join(sorted(bag.dags))}")
sys.exit(1 if failed else 0)
