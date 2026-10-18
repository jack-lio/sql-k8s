# Python 3.10 build of the same Airflow version: the Anthropic SDK 1.x used by
# 08.aqi-pipeline-health.py needs Python 3.10+ (the default 2.6.1 image is 3.7).
FROM apache/airflow:2.6.1-python3.10

# The SDK lives in its own virtualenv, run by ExternalPythonOperator: installed
# next to Airflow it upgrades h11 past what Airflow's httpcore allows.
# PIP_USER=false: the Airflow image sets PIP_USER=true, which venvs reject.
RUN python -m venv /home/airflow/anthropic-venv \
    && PIP_USER=false /home/airflow/anthropic-venv/bin/pip install --no-cache-dir "anthropic==1.15.0"

COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/
