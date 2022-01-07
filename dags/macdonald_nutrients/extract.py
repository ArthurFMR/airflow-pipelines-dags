import requests

from scrapper import get_all_food_ids


# Get Food json data
def fetch_food_data(food_id):
    url = f"https://www.mcdonalds.com/wws/json/getItemDetails.htm?country=US&language=en&showLiveData=true&item={food_id}"
    data = requests.get(url).json()
    return data


def fetch_all_food_data():
    food_ids = get_all_food_ids()

    foods_list = []

    for food_id in food_ids:
        data = fetch_food_data(food_id)
        foods_list.append(data)
    
    return foods_list


if __name__ == '__main__':
    print(fetch_all_food_data())




