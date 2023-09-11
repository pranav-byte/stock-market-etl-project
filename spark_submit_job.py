from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
)
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gcp_dataproc_spark_job',
    default_args=default_args,
    description='A DAG to run Spark job on Dataproc',
    start_date=datetime(2023, 9, 3),
    tags=['example'],
)



submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_job',
    main='gs://stock-extracted-data-bucket/code/sparkcode.py',
    cluster_name='spark-airflow-job',
    region='us-central1',
    project_id='tactical-helix-395106',
    dag=dag,
)


move_gcs_files = BashOperator(
    task_id='move_gcs_files',
    bash_command='gsutil -m mv gs://stock-extracted-data-bucket/landing/top-gainers/newfile/* gs://stock-extracted-data-bucket/archived/',
    dag=dag,
)

submit_pyspark_job >> move_gcs_files