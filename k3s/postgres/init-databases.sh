#!/bin/sh
# Runs once, when the data volume is empty. Later password changes in
# secrets.env are NOT applied to existing users; use ALTER USER for that.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<EOSQL
CREATE USER airflow WITH PASSWORD '$AIRFLOW_DB_PASSWORD';
CREATE DATABASE airflow OWNER airflow;
CREATE USER airflow_data WITH PASSWORD '$AIRFLOW_DATA_DB_PASSWORD';
CREATE DATABASE airflow_data OWNER airflow_data;
EOSQL
