#!/usr/bin/env bash
# Create or update the airflow-dag-secrets Secret that every Airflow pod loads
# as environment variables (extraEnvFrom in airflow-values.yaml):
#
#   MOENV_API_KEY                  from api-keys.env
#   AIRFLOW_CONN_MINIO_AQI         from minio/secrets.env (connection "minio_aqi")
#   ANTHROPIC_API_KEY              optional, from anthropic.env; without it the
#                                  aqi_pipeline_health DAG skips the AI review
#
# Usage: ./airflow-secrets.sh [--restart]
#   --restart  restart the scheduler and worker so they read the new values
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
set -a
# shellcheck source=/dev/null
source "$ROOT/api-keys.env"
# shellcheck source=/dev/null
source "$ROOT/minio/secrets.env"
if [[ -f "$ROOT/anthropic.env" ]]; then
    # shellcheck source=/dev/null
    source "$ROOT/anthropic.env"
fi
set +a

conn=$(printf '{"conn_type": "aws", "extra": {"aws_access_key_id": "%s", "aws_secret_access_key": "%s", "endpoint_url": "http://minio.minio.svc:9000", "region_name": "us-east-1", "config_kwargs": {"s3": {"addressing_style": "path"}}}}' \
    "$AIRFLOW_ACCESS_KEY" "$AIRFLOW_SECRET_KEY")

args=(
    --from-literal=MOENV_API_KEY="$MOENV_API_KEY"
    --from-literal=AIRFLOW_CONN_MINIO_AQI="$conn"
)
if [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
    args+=(--from-literal=ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY")
    echo "ANTHROPIC_API_KEY: included"
else
    echo "ANTHROPIC_API_KEY: not set (create $ROOT/anthropic.env to enable the AI review)"
fi

kubectl -n airflow create secret generic airflow-dag-secrets "${args[@]}" \
    --dry-run=client -o yaml | kubectl apply -f -

if [[ "${1:-}" == "--restart" ]]; then
    kubectl -n airflow rollout restart statefulset/airflow-scheduler
    kubectl -n airflow rollout status statefulset/airflow-scheduler --timeout=5m
fi
