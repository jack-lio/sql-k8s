# AQI data platform on Kubernetes

Airflow pipelines that collect Taiwan air-quality data and station images,
run image-based AQI prediction, and check the health of the whole pipeline
with Claude. Deployed with GitOps: GitHub Actions builds and tests, ArgoCD
running in the cluster deploys.

```
 git push ──► GitHub Actions ─────────────────────────────┐
              validate: kustomize/helm render, kubeconform, │
                        promtool, dashboard JSON            │
              image:    build Airflow image, load all DAGs, │
                        push ghcr.io/<owner>/<repo>/airflow │
              deploy:   commit the new tag to               │
                        airflow-values.yaml                 ▼
                                                     GitHub repository
                                                            │ polled every ~3 min
 minikube ◄──── ArgoCD (app of apps: argocd/root.yaml) ◄────┘
   ├─ databases   k8s/          Postgres x2, storage quotas
   ├─ minio       minio/        images + reports bucket
   ├─ monitoring  monitoring/   Prometheus, exporters, Grafana
   ├─ airflow     Helm chart 1.10.0 + airflow-values.yaml
   └─ vqaweb      k8s/ in jack-lio/VQAweb (prediction service, own CI)
```

The cluster pulls from GitHub, so it needs no inbound access: GitHub's
runners never reach minikube.

## Pipelines (dags/airflow-dags)

| DAG | Schedule | What it does |
|---|---|---|
| `get_moenv_aqi_info_v2` (04) | hourly | MOENV + WAQI AQI into Postgres `aqi4_info` |
| `aqi_image_predict` (07) | :20 hourly | station images to MinIO, weather values, prediction via `vqaweb`, into `aqi_prediction` |
| `aqi_pipeline_health` (08) | every 6 h | collects DAG, database, MinIO and alert signals; Claude reviews them; report to Postgres and MinIO |

## Secrets

Nothing secret is committed (`.gitignore` excludes every `*.env`).
`bootstrap-secrets.sh` creates the Kubernetes Secrets from local files:

| File | Secret |
|---|---|
| `k3s/secrets.env` (`MOENV_API_KEY`, `WAQI_TOKEN`) | `airflow/airflow-dag-secrets` |
| `anthropic.env` (`ANTHROPIC_API_KEY`, optional) | `airflow/airflow-dag-secrets` |
| `minio/secrets.env` (generated) | `minio/minio-credentials`, Airflow connection `minio_aqi` |
| `monitoring/secrets.env` (generated) | `monitoring-db` in `monitoring` and `sql` |
| `ghcr.env` (`GHCR_USER`, `GHCR_TOKEN`, only for a private image) | `airflow/ghcr-pull` |
| `argocd/repo.env` (`GITHUB_USER`, `GITHUB_TOKEN`, only for a private repo) | ArgoCD repository credentials |

## New cluster

```bash
minikube start
./bootstrap-secrets.sh          # Secrets that git does not hold
./argocd/bootstrap.sh           # ArgoCD + the root Application
```

ArgoCD then deploys everything else, including the prediction service
`vqaweb` (namespace `default`) from the jack-lio/VQAweb repository.

## Day to day

- Change a DAG or the Dockerfile, push to `main`: CI builds and tests the
  image, pushes it, bumps the tag; ArgoCD rolls Airflow.
- Change a manifest, dashboard or alert rule, push to `main`: CI validates;
  ArgoCD applies it. Generated ConfigMaps get a new hash, so Prometheus and
  Grafana restart with the new files, and ArgoCD prunes the old ones.
- Pull requests run the same checks without pushing or deploying.
- Changed an env file: `./bootstrap-secrets.sh --restart`.

ArgoCD UI: `kubectl -n argocd port-forward svc/argocd-server 8081:80`, user
`admin`, password from
`kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d`.

`k3s/` holds the Raspberry Pi variant and is not deployed by ArgoCD.
