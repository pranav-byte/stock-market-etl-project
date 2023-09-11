from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# Define default_args and other configurations for your main DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the main DAG
main_dag = DAG(
    'main_dag',
    default_args=default_args,
    schedule_interval="0 17 * * *",
)

		
def check_gcs_file():
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket('stock-extracted-data-bucket')
    blobs = list(bucket.list_blobs(prefix='/landing/top_gainers/newfile/'))
    c=0
    for i in blobs:
        c=c+1

    print(c)
    if c>0:
        return 'trigger_secondary_dag'  
    else:
        return 'end_dag'  


start_task = DummyOperator(
    task_id='start_task',
    dag=main_dag,
)

branch_task = PythonOperator(
    task_id='branch_task',
    python_callable=check_gcs_file,
    provide_context=True,
    dag=main_dag,
)

trigger_secondary_dag = TriggerDagRunOperator(
    task_id='trigger_secondary_dag',
    trigger_dag_id='gcp_dataproc_spark_job',  
    dag=main_dag,
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=main_dag,
)

# Define task dependencies
start_task >> branch_task >> [trigger_secondary_dag, end_dag]