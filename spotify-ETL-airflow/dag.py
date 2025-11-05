from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='mysql_etl_dag',  
    default_args=default_args,
    description='A simple ETL DAG to run the Spotify ETL script',
    schedule_interval=timedelta(minutes=5),  
    start_date=datetime(2023, 7, 21),
    catchup=False,  
)

run_etl = BashOperator(
    task_id='run_etl',
    bash_command="bash -c '/mnt/d/surendhar/projects/ETL-hadoop/spotify_etl_script.sh'",
    dag=dag,
)
