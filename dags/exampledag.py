from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import time
import logging
logging.getLogger("airflow.hooks.base").setLevel(logging.ERROR)

default_args = {
    'owner': 'equipo_13',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}

def descargar_archivos_recaudacion(**kwards):
    return

with DAG(
    dag_id='recaudacion-arca',
    description='DAG ETL que recopila datos de la recaudación realizada por ARCA desde 2008 hasta 2025',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['arca', 'recaudacion', 'etl']
) as dag:

    task_descargar_archivos_recaudacion = PythonOperator(
        task_id = "task_descargar_archivos_recaudacion",
        python_callable = descargar_archivos_recaudacion
    )

    task_descargar_archivos_recaudacion
