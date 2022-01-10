import pandas as pd
from airflow.decorators import task

from utils import _build_files_path

URL = "https://data.gov.au/data/dataset/ad89b4ff-541a-4729-b93c-4d2f5682e4c8/resource/809c77d8-fd68-4a2c-806f-c63d64e69842/download/airline_portcountry.csv"


@task(task_id="extract")
def fetch_data():
    # Limit quantity of records because free heroku tier allows 10,000.
    # and left free space for anothers datasets examples
    dataframe = pd.read_csv(URL, nrows=5000)

    path = _build_files_path('airlines.csv')

    dataframe.to_csv(path)

    return path
