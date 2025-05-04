from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 26),
    'depends_on_past': True,
    
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('fetch_cricket_stats',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval="@once",  # run just one time ' i will set it to @daily if i want the process to be daily,
          catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='run_script',
        bash_command='python /home/airflow/gcs/dags/scripts/extract_and_push_GCS.py',
    )
    