import pandas as pd
from airflow.decorators import task


@task(task_id="transform")
def transform(path:str):
    dataframe = pd.read_csv(path)

    columns_to_remove = ['Month', 'Freight_In_(tonnes)', 'Mail_In_(tonnes)', 'Freight_Out_(tonnes)',
                        'Mail_Out_(tonnes)']

    transformed_df = dataframe.drop(columns_to_remove, axis=1)
    transformed_df.columns = map(str.lower, transformed_df.columns)

    transformed_df.to_csv(path)

    return path