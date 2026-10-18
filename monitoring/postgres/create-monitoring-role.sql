-- Read-only login for postgres-exporter and Grafana. Run in each database by
-- the monitoring-role Job; safe to run again (also resets the password).
-- psql variable :'password' is passed with -v password=...

SELECT format('CREATE ROLE monitoring LOGIN PASSWORD %L', :'password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'monitoring') \gexec

ALTER ROLE monitoring WITH LOGIN PASSWORD :'password';

-- Server statistics (pg_stat_*, database sizes).
GRANT pg_monitor TO monitoring;

-- Read the application tables, including ones the DAGs create later.
GRANT CONNECT ON DATABASE :"dbname" TO monitoring;
GRANT USAGE ON SCHEMA public TO monitoring;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO monitoring;
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA public GRANT SELECT ON TABLES TO monitoring;
