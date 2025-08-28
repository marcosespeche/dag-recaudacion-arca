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

INITIAL_YEAR = 2008
LAST_YEAR = 2025
BASE_URL_ARCHIVO_RECAUDACION = "https://contenidos.afip.gob.ar/institucional/estudios/archivos/serie"
PATH_ARCHIVOS_RECAUDACION = "/tmp/recaudacion/"
PATH_ARCHIVOS_IPC = "/tpm/ipc/"
PATH_ARCHIVOS_EMAE = "/tpm/emae/"
PATH_ARCHIVOS_OUTPUT = "/tpm/result/"

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
    dag_id='montos',
    description='DAG ETL que recopila datos económicos en Argentina y de la recaudación realizada por ARCA desde 2008 hasta 2025',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['arca', 'recaudacion', 'etl']
) as dag:

    task_descargar_archivos_recaudacion = PythonOperator(
        task_id = "task_descargar_archivos_recaudacion",
        python_callable = descargar_archivos_recaudacion
    )

    [task_descargar_ipc_argentina, task_descargar_ipc_gba, task_descargar_ipc_cordoba, task_descargar_ipc_santafe, task_descargar_ipc_mendoza, task_descargar_ipc_tucuman, task_descargar_archivos_recaudacion, task_descargar_emae] >> task_merge

# El csv para la poblacion zona (String), poblacion, solo para las provincias que tengamos IPC

# En el IPC de GBA hay que sumar dos valores distintos porque salen desagregados
# Los csv para los IPC contienen las columnas anio, mes (1 al 12), valor (completo, sin redondear ni truncar), solo entre enero 2008 hasta julio 2025
# Los csv para el merge final contienen: anio, mes, recaudacion, ipc, emae