from datetime import timedelta
from airflow.models.dag import DAG
from airflow.utils.timezone import datetime
from astro_databricks import DatabricksNotebookOperator

default_args = {
    "retries": 2,
    'retry_delay': timedelta(minutes=5),
}

DATABRICKS_CONN_ID = "databricks_conn"


dag = DAG(
    dag_id="segmentation_customer_databricks",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["databricks"],
)
with dag:
    notebook_1 = DatabricksNotebookOperator(
        task_id="notebook_kmean_model_train",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_path="/Workspace/Users/trinhhagiaphu1905@gmail.com/kmean_model_train",
        source="WORKSPACE",
        existing_cluster_id="0528-140924-r1br6nab",
    )