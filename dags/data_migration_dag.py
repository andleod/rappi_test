from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sys
import os

# Asegurar que el directorio de scripts esté en el path de Python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.data_migration_flow import (
    setup_database,
    transform_records,
    get_imbalanced_transactions,
    get_account_summary,
)


# --- Monitoreo y Reporte ---
def task_failure_alert(context):
    """Función callback que se ejecuta cuando una tarea falla."""
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url

    # Imprime un mensaje de alerta en la consola de Airflow
    print(f"""
    --------------------------------------------------------------------
    ALERTA DE FALLO EN AIRFLOW
    --------------------------------------------------------------------
    DAG: {dag_id}
    Tarea: {task_id}
    Fecha de ejecución: {execution_date}

    Error: La tarea ha fallado.

    Consulte los logs para más detalles:
    {log_url}
    --------------------------------------------------------------------
    """)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 2),
    'retries': 1,
    'on_failure_callback': task_failure_alert,  # Callback para todas las tareas
}


def generate_report(**kwargs):
    """Toma los resultados de tareas anteriores vía XComs y crea un archivo de reporte."""
    ti = kwargs['ti']

    # Obtener DataFrames (en formato JSON) desde XComs
    imbalanced_df_json = ti.xcom_pull(task_ids='validate_balances', key='return_value')
    summary_df_json = ti.xcom_pull(task_ids='generate_account_summary', key='return_value')

    # Convertir JSON de vuelta a DataFrames
    imbalanced_df = pd.read_json(imbalanced_df_json)
    summary_df = pd.read_json(summary_df_json)

    # Crear el contenido del reporte
    report_content = "=== Reporte de Migración de Datos Financieros ===\n\n"

    report_content += "--- Transacciones Desbalanceadas ---\n"
    report_content += imbalanced_df.to_string(index=False)
    report_content += "\n\n"

    report_content += "--- Resumen de Saldos por Cuenta (Transacciones Válidas) ---\n"
    report_content += summary_df.to_string(index=False)
    report_content += "\n"

    # Escribir el reporte a un archivo
    report_path = 'output/final_report.txt'
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, 'w') as f:
        f.write(report_content)

    print(f"Reporte generado exitosamente en {report_path}")


with DAG(
        dag_id='financial_data_migration_dag',
        default_args=default_args,
        description='ETL para la migración de datos financieros.',
        schedule_interval=None,  # Para ejecución manual
        catchup=False,
        tags=['data-challenge', 'etl'],
) as dag:
    setup_task = PythonOperator(
        task_id='setup_database',
        python_callable=setup_database,
    )

    transform_task = PythonOperator(
        task_id='transform_records',
        python_callable=transform_records,
    )

    validate_balances_task = PythonOperator(
        task_id='validate_balances',
        python_callable=lambda: get_imbalanced_transactions().to_json(),
    )

    generate_account_summary_task = PythonOperator(
        task_id='generate_account_summary',
        python_callable=lambda: get_account_summary().to_json(),
    )

    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )

    # Definir dependencias
    setup_task >> transform_task
    transform_task >> [validate_balances_task, generate_account_summary_task]
    [validate_balances_task, generate_account_summary_task] >> generate_report_task