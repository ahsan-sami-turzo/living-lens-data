from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# These are the scripts you mentioned, assumed to be in the same directory as your DAG file
from your_script1 import main as script1_main
from your_script2 import main as script2_main
from your_script3 import main as script3_main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'your_dag_id',
    default_args=default_args,
    description='A simple DAG to run Python scripts sequentially',
    schedule_interval='0 23 * * 5',  # At 23:00 on Friday
    start_date=days_ago(1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='get_data',
    python_callable=script1_main,
    dag=dag,
)

t2 = PythonOperator(
    task_id='convert_to_excel',
    python_callable=script2_main,
    dag=dag,
)

t3 = PythonOperator(
    task_id='store_in_database',
    python_callable=script3_main,
    dag=dag,
)

t1 >> t2 >> t3  # Defines the order of task execution
