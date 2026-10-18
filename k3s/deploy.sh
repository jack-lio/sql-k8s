#!/usr/bin/env bash
# Deploy Postgres + Airflow (with the AQI DAGs) to k3s on the Raspberry Pi.
#
# Usage: PI_HOST=pi@192.168.1.50 ./k3s/deploy.sh [tag]
#
# Needs on this machine: docker buildx with QEMU arm64 emulation, kubectl,
# helm, the Pi's kubeconfig at $PI_KUBECONFIG, and k3s/secrets.env.
# Needs on the Pi: passwordless sudo for PI_HOST (the Raspberry Pi OS default).
set -euo pipefail

PI_HOST="${PI_HOST:?set PI_HOST, e.g. PI_HOST=pi@192.168.1.50}"
PLATFORM="${PLATFORM:-linux/arm64}"
export KUBECONFIG="${PI_KUBECONFIG:-$HOME/.kube/pi.yaml}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TAG="${1:-$(date +%Y%m%d%H%M)}"
CHART_VERSION=1.10.0

set -a
# shellcheck source=/dev/null
source "$ROOT/k3s/secrets.env"
set +a

# Create or update a Secret from key=value pairs.
apply_secret() {
    local namespace="$1" name="$2"
    shift 2
    local args=()
    for pair in "$@"; do
        args+=(--from-literal="$pair")
    done
    kubectl -n "$namespace" create secret generic "$name" "${args[@]}" \
        --dry-run=client -o yaml | kubectl apply -f -
}

echo "==> Building airflow-custom:$TAG ($PLATFORM)"
docker buildx build --platform "$PLATFORM" --load \
    -t "airflow-custom:$TAG" "$ROOT"

echo "==> Importing image into k3s on $PI_HOST"
docker save "airflow-custom:$TAG" | ssh "$PI_HOST" sudo k3s ctr images import -

echo "==> Postgres"
kubectl apply -f "$ROOT/k3s/postgres/namespace.yaml"
apply_secret sql postgres-credentials \
    "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" \
    "AIRFLOW_DB_PASSWORD=$AIRFLOW_DB_PASSWORD" \
    "AIRFLOW_DATA_DB_PASSWORD=$AIRFLOW_DATA_DB_PASSWORD"
kubectl apply -k "$ROOT/k3s/postgres"
kubectl -n sql rollout status statefulset/postgres --timeout=5m

echo "==> Airflow"
DB_HOST=postgres.sql.svc.cluster.local:5432
kubectl create namespace airflow --dry-run=client -o yaml | kubectl apply -f -
apply_secret airflow airflow-metadata \
    "connection=postgresql://airflow:$AIRFLOW_DB_PASSWORD@$DB_HOST/airflow"
apply_secret airflow airflow-fernet-key "fernet-key=$AIRFLOW_FERNET_KEY"
apply_secret airflow airflow-webserver-secret-key \
    "webserver-secret-key=$AIRFLOW_WEBSERVER_SECRET_KEY"
apply_secret airflow airflow-dag-secrets \
    "AIRFLOW_CONN_POSTGRES_DEFAULT=postgres://airflow_data:$AIRFLOW_DATA_DB_PASSWORD@$DB_HOST/airflow_data" \
    "MOENV_API_KEY=$MOENV_API_KEY" \
    "WAQI_TOKEN=$WAQI_TOKEN"

helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
# With useHelmHooks: false these are plain Jobs, and a Job's pod template is
# immutable, so a new image tag would make the upgrade fail. Recreate them.
kubectl -n airflow delete job airflow-run-airflow-migrations airflow-create-user \
    --ignore-not-found
helm upgrade --install airflow apache-airflow/airflow \
    --version "$CHART_VERSION" \
    --namespace airflow \
    -f "$ROOT/k3s/airflow-values-pi.yaml" \
    --set images.airflow.tag="$TAG" \
    --set webserver.defaultUser.password="$AIRFLOW_ADMIN_PASSWORD" \
    --wait --timeout 20m

echo "==> Done: http://${PI_HOST#*@}:30080/ (user airflow, password AIRFLOW_ADMIN_PASSWORD in k3s/secrets.env)"
