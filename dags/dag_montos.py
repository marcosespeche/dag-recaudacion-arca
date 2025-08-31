from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO
import re
from pathlib import Path
import os
import json
import time
from datetime import datetime, timedelta
import logging
import xlrd
from openpyxl import load_workbook
logging.getLogger("airflow.hooks.base").setLevel(logging.ERROR)

INITIAL_YEAR = 2008
LAST_YEAR = 2025
URL_ARCHIVO_IPC_ARGENTINA = "/ftp/cuadros/economia/sh_ipc_08_25.xls"
URL_ARCHIVO_IPC_CORDOBA = "/dataset/fedc5285-5517-41aa-9095-bb62c6dbc485/resource/2b4a7c60-1c8a-45b1-be8f-2bd59bfe2364/download/ipc-cba-julio.xlsx"
URL_ARCHIVO_RECAUDACION = "/institucional/estudios/archivos/serie"
PATH_ARCHIVOS_RECAUDACION = "/tmp/recaudacion/"
PATH_ARCHIVOS_IPC = "/tmp/ipc/"
PATH_ARCHIVOS_EMAE = "/tmp/emae/"
PATH_ARCHIVOS_OUTPUT = "/tmp/result/"

default_args = {
    'owner': 'equipo_13',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}

# Función que trae una hoja de un archivo Excel (.xls)
def obtener_hoja_xls(hook, endpoint, nombre_hoja):
    res = hook.run(endpoint)
    data = BytesIO(res.content)

    workbook = xlrd.open_workbook(file_contents=res.content)
    sheet = workbook.sheet_by_name(nombre_hoja)

    data = []
    for row_idx in range(sheet.nrows):
        row = []
        for col_idx in range(sheet.ncols):
            cell_value = sheet.cell_value(row_idx, col_idx)
            row.append(cell_value)
        data.append(row)    
    
    return data

# Función que trae una hoja de un archivo Excel (.xlsx)
def obtener_hoja_xlsx(hook, endpoint, nombre_hoja):
   res = hook.run(endpoint)
   data = BytesIO(res.content)

   workbook = load_workbook(data,data_only=True)
   sheet = workbook[nombre_hoja]

   data = []
   for row in sheet.iter_rows(values_only=True):
       data.append(list(row))
   
   return data

class CSVExporter:
    def __init__(self):
        self.values = []
    
    def add(self, anio, mes, valor):
        if not isinstance(anio, int) or anio < 1900 or anio > 2025:
            raise Exception(f"El año {anio} no es un número entero o no es válido")
        if not isinstance(mes, int) or mes < 1 or mes > 12:
            raise Exception(f"El mes {mes} no es un número entero o no es válido")
        if not isinstance(valor, (int, float)):
            raise Exception(f"El valor {valor} no es válido")
        
        self.values.append({
            "anio": anio,
            "mes": mes,
            "valor": valor
        })

    def export(self, path, name):
        df = pd.DataFrame(self.values)
        if not df.empty:
            df = df.sort_values(['anio', 'mes'])
        
        Path(path).mkdir(parents=True, exist_ok=True)
        ruta = f"{path}{name}.csv"
        df.to_csv(ruta, index=False)

        return ruta


def descargar_ipc_argentina(**kwards):
    hook = HttpHook(http_conn_id='indec', method='GET')
    
    try:
        source = obtener_hoja_xls(hook, URL_ARCHIVO_IPC_ARGENTINA, "Variación mensual IPC Nacional")

        # Encontrar filas de periodo y de nivel general
        nombre_fila_periodo = "Total nacional"
        ix_fila_periodo = None
        nombre_fila_nivel_general = "Nivel general"
        ix_fila_nivel_general = None
        for index, row in enumerate(source):
            if row[0] == nombre_fila_periodo:
                ix_fila_periodo = index
                continue
            if row[0] == nombre_fila_nivel_general:
                ix_fila_nivel_general = index
            if ix_fila_periodo is not None and ix_fila_nivel_general is not None:
                break
        if (ix_fila_periodo is None):
            raise Exception("No se encontró la fila con los periodos")
        if (ix_fila_nivel_general is None):
            raise Exception("No se encontró la fila con los valores del nivel general")

        # Encontrar valores del IPC
        exporter = CSVExporter()
        for index, column in enumerate(zip(*source)):
            periodo = column[ix_fila_periodo]
            
            if not (isinstance(periodo, (int, float)) and periodo > 0):
                continue
            if periodo >= 60:
                date_obj = datetime(1899, 12, 30) + timedelta(days=periodo)
            else: 
                date_obj = datetime(1899, 12, 31) + timedelta(days=periodo)

            anio = date_obj.year
            mes = date_obj.month
            valor = float(column[ix_fila_nivel_general]) / 100
            
            exporter.add(anio, mes, valor)

        # Convertir a CSV
        ruta = exporter.export(PATH_ARCHIVOS_IPC, "argentina")

        return ruta

    except Exception as e:
        logging.error(f"[ERROR] Problema al descargar IPC de Argentina: {e}")
        raise e


def descargar_ipc_cordoba(**kwards):
    hook = HttpHook(http_conn_id='estadisticacordoba', method='GET')

    try:
        source = obtener_hoja_xlsx(hook, URL_ARCHIVO_IPC_CORDOBA, "Variaciones Mensuales")
        # Encontrar filas de periodo y de nivel general
        nombre_fila_periodo = "Descripción"
        ix_fila_periodo = None
        nombre_fila_nivel_general = "NIVEL GENERAL"
        ix_fila_nivel_general = None
        for index, row in enumerate(source):
            if row[2] == nombre_fila_periodo:
                ix_fila_periodo = index
                continue
            if row[2] == nombre_fila_nivel_general:
                ix_fila_nivel_general = index
            if ix_fila_periodo is not None and ix_fila_nivel_general is not None:
                break
        if (ix_fila_periodo is None):
            raise Exception("No se encontró la fila con los periodos")
        if (ix_fila_nivel_general is None):
            raise Exception("No se encontró la fila con los valores del nivel general")

        # Encontrar valores del IPC
        exporter = CSVExporter()
        for index, column in enumerate(zip(*source)):
            periodo = column[ix_fila_periodo]
                        
            if not isinstance(periodo, datetime):
                continue

            anio = periodo.year
            mes = periodo.month
            valor = float(column[ix_fila_nivel_general])
            
            exporter.add(anio, mes, valor)

        # Convertir a CSV
        ruta = exporter.export(PATH_ARCHIVOS_IPC, "cordoba")

        return ruta

    except Exception as e:
        logging.error(f"[ERROR] Problema al descargar IPC de Córdoba: {e}")
        raise e

def descargar_archivos_recaudacion(**kwards):
    hook = HttpHook(http_conn_id='recaudacion-arca', method='GET')
    csv_exporter = CSVExporter()
    try:
        for year in range(INITIAL_YEAR, LAST_YEAR + 1):
            data = obtener_hoja_xls(hook, f"{URL_ARCHIVO_RECAUDACION}{year}.xls", f"{year}")

            nombre_fila_recaudacion = '  TOTAL GENERAL'
            idx_fila_recaudacion = None

            for index, row in enumerate(data):
                if (row[1] != nombre_fila_recaudacion):
                    continue
                idx_fila_recaudacion = index

                for month in range(1, 13):
                    column_index = month + 1
                    recaudacion = row[column_index]
                    if year < 2021:
                        # Antes de 2021: convertir de miles a millones
                        recaudacion = recaudacion / 1000
                    if recaudacion is not None and recaudacion != '':
                        # No agarra el valor total (la sumatoria de todos los meses) porque está definido como una función de Excel, no es un valor numerico en sí
                        csv_exporter.add(year, month, recaudacion)
                break

            if idx_fila_recaudacion is None:
                logging.warning(f"No se encontró la fila con los totales de recaudación para el año {year}")

        return csv_exporter.export(PATH_ARCHIVOS_RECAUDACION, 'recaudacion-arca')
    
    except Exception as e:
        logging.error(f"[ERROR] Problema al descargar la recaudación de ARCA: {e}")
        raise e

def descargar_emae(**kwards):
    hook = HttpHook(http_conn_id='emae', method='GET')
    csv_exporter = CSVExporter()
    
    MESES_MAP = { 'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4, 'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8, 'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12 }

    current_year = None
    meses_por_anio = {}

    try:
        data = obtener_hoja_xls(hook, 'sh_emae_mensual_base2004.xls', 'EMAE')

        """
            Formato del Excel:

            2024 Enero valor1
                 Febrero valor 2
                 marzo valor3
            ...
            2025 Enero valor4

            Es decir, hay filas que contienen el año y filas que no 
            """

        for index, row in enumerate(data):
            anio = row[0]
            mes = row[1] 
            valor = row[2]

            # Si aun no se llega a un año en curso, se saltea la fila
            if current_year is None and not (isinstance(anio, (int, float)) and INITIAL_YEAR <= int(anio) <= LAST_YEAR):
                continue

            # Si se detecta un nuevo año, se pisa el valor de current_year anterior y añaden los datos registrados hasta el momento en el CSV
            if isinstance(anio, (int, float)) and INITIAL_YEAR <= int(anio) <= LAST_YEAR:

                if current_year and meses_por_anio:
                    for m, v in meses_por_anio.items():
                        csv_exporter.add(current_year, m, v)
                    meses_por_anio.clear()

                current_year = int(anio)

            # Registrar valor del mes si es válido
            if mes and mes in MESES_MAP and valor is not None:
                mes_numero = MESES_MAP[mes]
                meses_por_anio[mes_numero] = valor

        # Volcar lo último acumulado
        if current_year and meses_por_anio:
            for m, v in meses_por_anio.items():
                csv_exporter.add(current_year, m, v)

    except Exception as e:
        logging.error(f"Error al descargar el EMAE: {e}")
        raise e

    return csv_exporter.export(PATH_ARCHIVOS_EMAE, 'emae')

with DAG(
    dag_id='montos',
    description='DAG ETL que recopila datos económicos en Argentina y de la recaudación realizada por ARCA desde 2008 hasta 2025',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['arca', 'recaudacion', 'etl']
) as dag:
    
    task_descargar_ipc_argentina = PythonOperator(
        task_id = "task_descargar_ipc_argentina",
        python_callable = descargar_ipc_argentina
    )

    task_descargar_ipc_cordoba = PythonOperator(
        task_id = "task_descargar_ipc_cordoba",
        python_callable = descargar_ipc_cordoba
    )

    task_descargar_archivos_recaudacion = PythonOperator(
        task_id = "task_descargar_archivos_recaudacion",
        python_callable = descargar_archivos_recaudacion
    )

    task_descargar_emae = PythonOperator(
        task_id = 'task_descargar_emae',
        python_callable = descargar_emae
    )

    [task_descargar_ipc_argentina, task_descargar_ipc_cordoba, task_descargar_archivos_recaudacion, task_descargar_emae]
    # [task_descargar_ipc_argentina, task_descargar_ipc_gba, task_descargar_ipc_cordoba, task_descargar_ipc_santafe, task_descargar_ipc_mendoza, task_descargar_ipc_tucuman, task_descargar_archivos_recaudacion, task_descargar_emae] >> task_merge

# El csv para la poblacion zona (String), poblacion, solo para las provincias que tengamos IPC

# En el IPC de GBA hay que sumar dos valores distintos porque salen desagregados
# Los csv para los IPC contienen las columnas anio, mes (1 al 12), valor (completo, sin redondear ni truncar), solo entre enero 2008 hasta julio 2025
# Los csv para el merge final contienen: anio, mes, recaudacion, ipc, emae