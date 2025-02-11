from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_etl_dag",
    default_args=default_args,
    description="Run Spark ETL Job",
    schedule_interval="@daily",
)

spark_etl_task = SparkSubmitOperator(
    task_id="run_spark_etl",
    application="/opt/airflow/dags/etl.py",
    conn_id="spark_default",
    executor_cores=2,
    executor_memory="1g",
    num_executors=2,
    verbose=True,
    dag=dag,
)

spark_etl_task
