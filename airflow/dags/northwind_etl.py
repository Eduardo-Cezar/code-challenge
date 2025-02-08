from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow/meltano')

from transform.extract_to_parquet import extract_to_parquet
from transform.load_to_db import load_to_db

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'northwind_etl',
    default_args=default_args,
    description='ETL Northwind',
    schedule_interval='0 2 * * *',  # executa as 02:00
    catchup=False  
)

def _extract_to_parquet(**context):
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    return extract_to_parquet(execution_date)

def _load_to_db(**context):
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    return load_to_db(execution_date)

extract_task = PythonOperator(
    task_id='extract_to_parquet',
    python_callable=_extract_to_parquet,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_db',
    python_callable=_load_to_db,
    provide_context=True,
    dag=dag,
)

# reprocessamento manual
reprocess_dag = DAG(
    'northwind_etl_reprocess',
    default_args=default_args,
    description='Reprocessar ETL Northwind para uma data específica',
    schedule_interval=None, )

def _reprocess(**context):
    execution_date = context['dag_run'].conf.get('execution_date')
    if not execution_date:
        raise ValueError("Data de execução não fornecida")
    
    print(f"Reprocessando data: {execution_date}")
    extract_to_parquet(execution_date)
    load_to_db(execution_date)

reprocess_task = PythonOperator(
    task_id='reprocess',
    python_callable=_reprocess,
    provide_context=True,
    dag=reprocess_dag,
)

extract_task >> load_task 