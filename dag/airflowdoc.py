import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

parent_folder = os.path.dirname(os.path.abspath(__file__))
sys.path.append(parent_folder)

from csvtopg import csvtopg_main
from framestopg import framestopg_main

start_date = datetime(2023, 1, 1, 12, 10)

default_args = {
    'owner': 'Anne Hathaway',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('csv_extract_airflow_docker', default_args=default_args, schedule=timedelta(days=1), catchup=False) as dag:
    write_csv_to_postgres = PythonOperator(
        task_id='csvtopg',
        python_callable=csvtopg_main,
        retries=1,
        retry_delay=timedelta(seconds=15))

    write_df_to_postgres = PythonOperator(
        task_id='framestopg',
        python_callable=framestopg_main,
        retries=1,
        retry_delay=timedelta(seconds=15))

    write_csv_to_postgres >> write_df_to_postgres
