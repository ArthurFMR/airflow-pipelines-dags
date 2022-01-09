from airflow import DAG

from datetime import datetime, timedelta

from macdonald_nutrients.extract import fetch_all_food_data
from macdonald_nutrients.transform import structure_all_foods_data
from macdonald_nutrients.load import load_data

default_args = {
    'owner': 'Arthur',
    'start_date': datetime.now() + timedelta(minutes=3),
    
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'macdonald_nutrients_pipeline', 
    default_args=default_args, 
    schedule_interval=timedelta(days=30)
    ) as dag:

    foods_list = fetch_all_food_data()
    transformed_foods_data_list = structure_all_foods_data(foods_list)
    load_data(transformed_foods_data_list)





