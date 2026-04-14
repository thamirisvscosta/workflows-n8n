from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

# 1. DAG Configuration
with DAG(
    dag_id='data_engineering_pipeline_v1',
    start_date=datetime(2026, 4, 13),
    schedule_interval=None, 
    catchup=False
) as dag:

    # 2. Functions (Pipeline Logic)
    def check_files():
        # This path refers to the internal Docker volume
        path = '/usr/local/airflow/data'
        files = os.listdir(path)
        print(f"Files found in local Data Lake: {files}")

    def connect_warehouse():
        # Future connection to MySQL (Data Warehouse)
        print("Simulating connection to MySQL at localhost:3306...")

    # 3. Tasks
    task_check = PythonOperator(
        task_id='check_local_lake',
        python_callable=check_files
    )

    task_db = PythonOperator(
        task_id='connect_mysql_warehouse',
        python_callable=connect_warehouse
    )

    # 4. Workflow
    task_check >> task_db