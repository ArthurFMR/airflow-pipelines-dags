from airflow import DAG

from datetime import datetime, timedelta

from international_airlines.extract import fetch_data
from international_airlines.transform import transform
from international_airlines.load import load_data

default_args = {
    'owner': 'Arthur',
    'start_date': datetime.now() + timedelta(minutes=3),
    
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    'international_airlines_pipeline', 
    default_args=default_args, 
    schedule_interval=timedelta(days=30),
    catchup=False
    ) as dag:

    airlines = fetch_data() # Return path to csv file
    airlines_transformed = transform(airlines)
    load_data(airlines_transformed) # The transformed data is saved in the same file
