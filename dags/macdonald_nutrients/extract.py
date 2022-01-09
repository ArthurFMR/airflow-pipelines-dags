import requests

from macdonald_nutrients.scrapper import get_all_food_ids
from utils import _save_to_file
from airflow.decorators import task


# Get Food json data
def fetch_food_data(food_id):
    url = f"https://www.mcdonalds.com/wws/json/getItemDetails.htm?country=US&language=en&showLiveData=true&item={food_id}"
    data = requests.get(url).json()
    return data

@task(task_id="extract")
def fetch_all_food_data():
    food_ids = get_all_food_ids()
    foods_list = []

    for food_id in food_ids:
        data = fetch_food_data(food_id)
        foods_list.append(data)
    
    path = _save_to_file('foods_list', foods_list)
    
    return path


if __name__ == '__main__':
    print(fetch_all_food_data())




