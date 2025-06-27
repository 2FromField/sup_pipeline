from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def my_simple_task():
    print("Exécution de ma tâche simple")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='Un exemple de DAG simple',
    schedule=timedelta(days=1),  
)

t1 = PythonOperator(
    task_id='simple_task',
    python_callable=my_simple_task,
    dag=dag,
)
