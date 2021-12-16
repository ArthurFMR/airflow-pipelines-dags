from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Arthur',
    'start_date': datetime.now() + timedelta(minutes=3),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

pipeline = DAG(
    'first_dag',
    'First Pipeline as DAG',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

task1 = BashOperator(
    task_id='print_hello',
    bash_command="echo \'greetings. the date and time are \'",
    dag=pipeline
)

task2 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=pipeline
)

task1 >> task2