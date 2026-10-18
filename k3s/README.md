# Postgres + Airflow on k3s (Raspberry Pi)

Runs the AQI DAGs from `dags/airflow-dags` on the same Pi as AQIweb. Do the
one-time setup in `~/AQIweb/k3s/README.md` first (k3s, `~/.kube/pi.yaml`,
buildx + arm64 emulation).

```text
namespace sql:      postgres (StatefulSet, 5Gi volume)
                      |- database airflow       Airflow metadata
                      `- database airflow_data  data written by the DAGs
namespace airflow:  scheduler (runs the tasks, LocalExecutor) + webserver (NodePort 30080)
```

Compared with the minikube setup (`../airflow-values.yaml`), the Pi version has
no Celery worker, Redis, statsd or triggerer, and no second Postgres, so it
uses about half the memory.

## Secrets

`secrets.env` holds the database passwords, the Airflow keys, the admin
password and the API keys (`MOENV_API_KEY`, `WAQI_TOKEN`). It was generated
locally, is excluded from the Airflow image by `../.dockerignore`, and must not
be committed. `secrets.env.example` lists the keys.

`deploy.sh` turns it into Kubernetes Secrets. The DAGs get:

- `AIRFLOW_CONN_POSTGRES_DEFAULT`: the `postgres_default` connection, pointing at `airflow_data`
- `MOENV_API_KEY`, `WAQI_TOKEN`: read by `04.aqi-info.py`

The database users are created only the first time Postgres starts with an
empty volume (`postgres/init-databases.sh`). Changing a password in
`secrets.env` later does not change it in Postgres.

## Deploy

```bash
PI_HOST=pi@192.168.1.50 ./k3s/deploy.sh
```

Then open `http://<pi-ip>:30080/` and log in as `airflow` with
`AIRFLOW_ADMIN_PASSWORD` from `secrets.env`. New DAGs start paused.

To look at the data:

```bash
export KUBECONFIG=~/.kube/pi.yaml
kubectl -n sql exec -it postgres-0 -- psql -U postgres -d airflow_data -c 'select * from aqi4_info'
```
