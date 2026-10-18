#!/usr/bin/env bash
# Create every Secret the cluster needs but git must not hold. ArgoCD deploys
# everything else from the repository; run this once on a new cluster (before
# argocd/bootstrap.sh) and again after changing any of the env files.
#
#   minio/secrets.env       minio/minio-credentials        (generated if missing)
#   monitoring/secrets.env  monitoring-db in monitoring, sql (generated if missing)
#   k3s/secrets.env         \
#   anthropic.env (optional) > airflow/airflow-dag-secrets  (airflow-secrets.sh)
#   ghcr.env (optional)     airflow/ghcr-pull, for a private GHCR image:
#                             GHCR_USER=<github user>
#                             GHCR_TOKEN=<token with read:packages>
#
# Also creates the Airflow Fernet key, Redis password and broker URL once
# (see airflow-values.yaml for why the chart must not generate them).
#
# Usage: ./bootstrap-secrets.sh [--restart]
#   --restart  restart the Airflow scheduler and worker to read new values
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"

# apply_secret <namespace> <name> <kubectl create secret generic args...>
apply_secret() {
    local ns=$1 name=$2
    shift 2
    kubectl -n "$ns" create secret generic "$name" "$@" --dry-run=client -o yaml \
        | kubectl apply -f -
}

for ns in airflow minio monitoring sql; do
    kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
done

umask 077
if [[ ! -f "$ROOT/minio/secrets.env" ]]; then
    cat > "$ROOT/minio/secrets.env" <<EOF
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=$(openssl rand -hex 16)
AIRFLOW_ACCESS_KEY=airflow-aqi
AIRFLOW_SECRET_KEY=$(openssl rand -hex 20)
EOF
    echo "==> Created minio/secrets.env"
fi
if [[ ! -f "$ROOT/monitoring/secrets.env" ]]; then
    echo "MONITORING_DB_PASSWORD=$(openssl rand -hex 16)" > "$ROOT/monitoring/secrets.env"
    echo "==> Created monitoring/secrets.env"
fi

echo "==> MinIO and monitoring"
apply_secret minio minio-credentials --from-env-file="$ROOT/minio/secrets.env"
apply_secret monitoring monitoring-db --from-env-file="$ROOT/monitoring/secrets.env"
apply_secret sql monitoring-db --from-env-file="$ROOT/monitoring/secrets.env"

echo "==> Airflow chart secrets (created once, never rotated)"
if ! kubectl -n airflow get secret airflow-fernet-key >/dev/null 2>&1; then
    fernet=$(python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')
    apply_secret airflow airflow-fernet-key --from-literal=fernet-key="$fernet"
fi
if ! kubectl -n airflow get secret airflow-redis-password >/dev/null 2>&1; then
    redis=$(openssl rand -hex 16)
    apply_secret airflow airflow-redis-password --from-literal=password="$redis"
    apply_secret airflow airflow-broker-url \
        --from-literal=connection="redis://:$redis@airflow-redis:6379/0"
fi

if [[ -f "$ROOT/ghcr.env" ]]; then
    echo "==> GHCR pull secret"
    # shellcheck source=/dev/null
    source "$ROOT/ghcr.env"
    kubectl -n airflow create secret docker-registry ghcr-pull \
        --docker-server=ghcr.io --docker-username="$GHCR_USER" --docker-password="$GHCR_TOKEN" \
        --dry-run=client -o yaml | kubectl apply -f -
fi

echo "==> Airflow DAG secrets"
"$ROOT/airflow-secrets.sh" "$@"
