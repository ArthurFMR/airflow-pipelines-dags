from airflow import DAG
from airflow.decorators import dag

from datetime import datetime, timedelta

from macdonald_nutrients.extract import fetch_all_food_data
from macdonald_nutrients.transform import structure_all_foods_data
from macdonald_nutrients.load import load_data

default_args = {
    'owner': 'Arthur',
    'start_date': datetime.now() + timedelta(minutes=3),
    
    'retry_delay': timedelta(minutes=1)
}

@dag(schedule_interval=None, start_date=datetime.now() + timedelta(minutes=3), catchup=False)
def macdonald_nutrients_etl():
    foods_list = fetch_all_food_data()
    transformed_foods_data_list = structure_all_foods_data(foods_list)
    load_data(transformed_foods_data_list)

macdonald_nutrients_etl_dag = macdonald_nutrients_etl()

# with DAG(
#     'macdonald_nutrients_pipeline', 
#     default_args=default_args, 
#     schedule_interval=timedelta(days=30),
#     catchup=False
#     ) as dag:

#     foods_list = fetch_all_food_data()
#     transformed_foods_data_list = structure_all_foods_data(foods_list)
#     load_data(transformed_foods_data_list)





