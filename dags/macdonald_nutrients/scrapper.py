import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.mcdonalds.com"
MENU_URL = BASE_URL + "/us/en-us/full-menu.html"

resp = requests.get(MENU_URL)
soup = BeautifulSoup(resp.content, "html.parser")

foods_content = soup.find(id='maincategorycontent')

# Get categories of food
category_divs = foods_content.find_all("div", class_="productListing")

# Removing combo categories
for index in [5, 7]: del category_divs[index]


def get_category_foods(category_div):
    # Getting food list items
    food_items = category_div.find_all('li', class_="mcd-category-page__item")
    return food_items


def get_food_id(food_li):
    # Getting data-at attribute that contains the id of the food
    data_attr = food_li.find('a', class_="mcd-category-page__item-link")['data-at']
    
    # Detecting valid value to apply the cleaning
    if ":" in data_attr:
        food_id = data_attr.split(":")[-2] # Cleaning the string to get the id of the food
        return food_id


def get_all_food_ids():

    food_ids_list = []

    for category_div in category_divs:
        category_foods = get_category_foods(category_div)

        for food_li in category_foods:
            food_id = get_food_id(food_li)
            # When the data is not valid return None, that is why we verify is return None to not add it to the list.
            if food_id != None:
                food_ids_list.append(food_id)

    food_ids_list = set(food_ids_list) # Removing repeated data
    food_ids_list = list(food_ids_list)

    return food_ids_list


if __name__ == '__main__':
    print(get_all_food_ids())