from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

class PostgresOp:
    def __init__(self, dag: DAG, postgres_conn_id: str):
        self.dag = dag
        self.postgres_conn_id = postgres_conn_id

    def create_table(self, sqlfilename: str):
        return PostgresOperator(
            task_id =
            "postgresop.create_table.{}".format(self._normalized_task_name(sqlfilename)),
            postgres_conn_id = self.postgres_conn_id,
            sql = sqlfilename,
            dag = self.dag,
        )

    def _normalized_task_name(self, instr: str):
        return instr.replace("/", "-")

    def bulk_insert_from_stmt(self, sql_file:str):
        import os
        return PostgresOperator(
            task_id="postgresop.bulk_insert_form_stmt",
            postgres_conn_id = self.postgres_conn_id,
            sql = os.path.basename(sql_file),
            dag=self.dag,
        )

    def generate_bulk_insert_stmt(self, table_name: str, from_task_id: str,
            out_sql_file:str, update_on_conflict: bool=False, on_conflict_key: str= None):
        return PythonOperator(
            task_id="postgresop.generate_bulk_insert_stmt",
            python_callable=self._generate_bulk_insert_stmt,
            op_kwargs={"table_name": table_name,
                "from_task_id": from_task_id,
                "out_sql_file": out_sql_file,
                "update_on_conflict": update_on_conflict,
                "on_conflict_key": on_conflict_key,
                },
            provide_context=True,
            do_xcom_push=True,
            dag=self.dag,
        )

    def _generate_bulk_insert_stmt(self, ti, table_name: str, from_task_id:
            str, out_sql_file: str, update_on_conflict: bool, on_conflict_key:str):
        jsonobjs = ti.xcom_pull(task_ids=from_task_id)
        if "value" not in jsonobjs:
            raise Exception("value are not here")
        jsonkeys = list(jsonobjs["value"][0].keys())
        jsonkeys.sort()

        keysstr = ",".join(jsonkeys)
        with open(out_sql_file, "w") as f:
            for obj in jsonobjs["value"]: 
                values = ["'{}'".format(obj[key]) for key in jsonkeys]
                valuesstr = ",".join(values)
                if update_on_conflict:
                    update_stmt_list = []
                    for key in jsonkeys:
                        update_stmt_list.append("{} = EXCLUDED.{}".format(key, key))
                    update_stmt_cond = ", ".join(update_stmt_list)
                    stmt = f"INSERT INTO {table_name} ({keysstr}) VALUES({valuesstr}) ON CONFLICT({on_conflict_key}) DO UPDATE SET {update_stmt_cond};"
                else:
                    stmt = f"INSERT INTO {table_name} ({keysstr}) VALUES({valuesstr}) ON CONFLICT DO NOTHING;"
                f.write("{}\n".format(stmt))
        return

    def fetchall_json(self, table_name: str):
        return PostgresOperator(
            task_id="postgresop.fetch_all_json",
            postgres_conn_id="postgres_default",
            sql="SELECT * FROM {};".format(table_name),
        )
    def execute_sql_task(self, sql: str, task_id: str):
        return PostgresOperator(
            task_id=task_id,
            postgres_conn_id=self.postgres_conn_id,
            sql=sql,
            dag=self.dag,
        )
