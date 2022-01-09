from airflow.decorators import task
import pandas as pd
from sqlalchemy import create_engine
import os

from dotenv import load_dotenv


from macdonald_nutrients.transform import structure_all_foods_data
from utils import _read_file, _del_file

load_dotenv()

DB_URL = os.environ.get('DATABASE_URL')


@task(task_id="load")
def load_data(path:str):
    transformed_data = _read_file(path)

    df = pd.DataFrame(transformed_data)
    engine = create_engine(DB_URL, echo=False)
    df.to_sql('macdonald_nutrients', con=engine, if_exists='replace')

    _del_file(path)



if __name__ == '__main__':
    load_data(structure_all_foods_data(structure_all_foods_data()))



