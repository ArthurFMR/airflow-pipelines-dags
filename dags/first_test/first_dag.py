from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

pipeline = DAG(
    'first_dag',
    'First Pipeline as DAG',
    start_date=datetime.now() + timedelta(minutes=3),
    schedule_interval=timedelta(days=1)
)

task1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=pipeline
)