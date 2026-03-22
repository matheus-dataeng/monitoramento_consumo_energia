import pendulum 
from airflow.operators.python import PythonOperator
from airflow import DAG
from src.bronze import extract
from src.silver import transform
from src.gold import build_metrics
from src.gold import load

with DAG(
    dag_id = "Consumo_Energia_2025",
    description = "Consumo de energia no Brasil em 2025",
    start_date = pendulum.datetime(2026, 3, 10, tz= "America/Sao_Paulo"),
    catchup = False,
    schedule = None
) as dag:
    
    extract_task = PythonOperator(
        task_id = "Extract",
        python_callable = extract
    )
    
    transform_task = PythonOperator(
        task_id = "Transform",
        python_callable = transform
    )
    
    build_metrics_task = PythonOperator(
        task_id = "Build Metrics",
        python_callable = build_metrics
    )
    
    load_task = PythonOperator(
        task_id = "Load",
        python_callable = load
    )
    
    extract_task >> transform_task >> build_metrics_task >> load_task 