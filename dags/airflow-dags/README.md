# airflow-dags
a collection of airflow dags


### testing ###

We leveraged docker-compose for setting up envirnoment for us to test. One can keep folders organized as follows:
```
├── dags
│   └── airflow-dags
├── docker-compose.yaml
├── logs
├── plugins
```
and the airflow-dags (e.g. this repo) can be placed under dags.

You can use the following command to create folders
```
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
cd ./dags && git clone https://github.com/footprintai/airflow-dags

// copy docker-compose file onto the top level of dirs
cp ./dags/airflow-dags/docker-compose.yaml ./
```

We placed additional postgres for storing application data (i.e. the output of each dags are placed under this databases)
You can use `docker inpsect` to lookup its IP and configure a connection via airflow UI for the communication.

##### init db #####

run the following command to init db

```
docker compose up airflow-init
```

##### run #####
```
docker compose up
```

##### login #####

```
http://localhost:8080

with account:airflow, password: airflow

```

##### clean up ######

```
docker compose down --volumes --rmi all
```
