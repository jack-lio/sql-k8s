from airflow import DAG
from airflow.operators.python import PythonOperator

class PandasOp:
    def __init__(self, dag: DAG):
        self.dag = dag

    def convert_json_to_csv(self, from_task_id: str):
        return PythonOperator(
            task_id = "pandasop.convert_josn_to_csv",
            python_callable=self._convert_json_to_csv,
            op_kwargs={"from_task_id": from_task_id},
            provide_context=True,
            do_xcom_push=True,
            dag = self.dag,
        )

    def _convert_json_to_csv(self, ti, from_task_id: str):
        jsonstr = ti.xcom_pull(task_ids=from_task_id)

        import pandas as pd

        df = pd.read_json(jsonstr["value"])
        value = df.to_csv(header=False, index = False, sep ='\t')
        return {"value": value}
