import pandas as pd
from sqlalchemy import create_engine
import os

from dotenv import load_dotenv
load_dotenv()

from transform import structure_all_foods_data


DB_URL = os.environ.get('DATABASE_URL')


def load_data(transformed_data:list):
    df = pd.DataFrame(transformed_data)
    engine = create_engine(DB_URL, echo=False)
    df.to_sql('macdonald_nutrients', con=engine, if_exists='replace ')


if __name__ == '__main__':
    load_data(structure_all_foods_data())



