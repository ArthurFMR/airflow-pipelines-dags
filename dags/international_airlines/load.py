from airflow.decorators import task
import pandas as pd
from sqlalchemy import create_engine
import os

from dotenv import load_dotenv

from utils import _del_file

load_dotenv()
DB_URL = os.environ.get('DATABASE_URL')


@task(task_id="load")
def load_data(path:str):
    transformed_df = pd.read_csv(path)
    
    engine = create_engine(DB_URL, echo=False)

    transformed_df.to_sql('airlines', con=engine, if_exists="replace")

    _del_file(path)