#!/usr/bin/env bash
# Install ArgoCD into the current kubectl context (minikube) and point it at
# this repository. Run ./bootstrap-secrets.sh first.
#
# Usage: ./argocd/bootstrap.sh
#
# Private repository: create argocd/repo.env (not committed) with
#   GITHUB_USER=<github user>
#   GITHUB_TOKEN=<fine-grained token, Contents: read-only, this repo only>
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_URL=$(awk '/repoURL:/{print $2; exit}' "$DIR/root.yaml")

echo "==> ArgoCD"
kubectl apply --server-side --force-conflicts -k "$DIR/install"
for d in argocd-server argocd-repo-server argocd-redis argocd-applicationset-controller; do
    kubectl -n argocd rollout status "deploy/$d" --timeout=5m
done
kubectl -n argocd rollout status statefulset/argocd-application-controller --timeout=5m

if [[ -f "$DIR/repo.env" ]]; then
    echo "==> Credentials for $REPO_URL"
    # shellcheck source=/dev/null
    source "$DIR/repo.env"
    kubectl -n argocd create secret generic repo-sql-k8s \
        --from-literal=type=git --from-literal=url="$REPO_URL" \
        --from-literal=username="$GITHUB_USER" --from-literal=password="$GITHUB_TOKEN" \
        --dry-run=client -o yaml \
        | kubectl label --local -f - argocd.argoproj.io/secret-type=repository -o yaml \
        | kubectl apply -f -
fi

echo "==> Root application"
kubectl apply -f "$DIR/root.yaml"

echo "==> Done"
echo "UI: kubectl -n argocd port-forward svc/argocd-server 8081:80, then http://localhost:8081"
echo "    user admin, password:"
echo "    kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d"
