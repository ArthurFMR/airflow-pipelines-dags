from airflow.decorators import task
from macdonald_nutrients.extract import fetch_all_food_data
from utils import _read_file, _save_to_file


def get_nutrient_value(nutrient_facts:dict, nutrient_id:str):
    nutrients = nutrient_facts['nutrient']

    for nutrient in nutrients:
        # find nutrient value matching nutrient id
        if nutrient['nutrient_name_id'] == nutrient_id:
            return float(nutrient['value']) # Converting  value to float


def structure_food_data(data:dict):
    nutrient_facts = data['nutrient_facts']

    # Around 5 food item does not offers nutrient_facts data, so we verify that, to not appended to the list
    # And Around 1 food item does not offers category data, so we remove it

    if len(nutrient_facts) != 0 and len(data['default_category']) != 0:
        data = {
            "food_name": data['item_name'],
            "category": data['default_category']['category']['name'],
            "calories_cal": get_nutrient_value(nutrient_facts, 'calories'),
            "total_fat_g": get_nutrient_value(nutrient_facts, 'fat'),
            "total_carbohydrates_g": get_nutrient_value(nutrient_facts, 'carbohydrate'),
            "protein_g": get_nutrient_value(nutrient_facts, 'protein'),
            "saturated_fat_g": get_nutrient_value(nutrient_facts, 'saturated_fat'),
            "dietary_fiber_g": get_nutrient_value(nutrient_facts, 'fibre'),
            "calcium": get_nutrient_value(nutrient_facts, 'calcium'),
            "trans_fat": get_nutrient_value(nutrient_facts, 'trans_fat'),
            "total_sugars": get_nutrient_value(nutrient_facts, 'sugars'),
            "iron": get_nutrient_value(nutrient_facts, 'iron'),
            "cholesterol": get_nutrient_value(nutrient_facts, 'cholesterol'),
            "vitaminD": get_nutrient_value(nutrient_facts, 'vitaminD'),
            "potassium": get_nutrient_value(nutrient_facts, 'potassium'),
            "sodium": get_nutrient_value(nutrient_facts, 'sodium'),
            "phosphorus": get_nutrient_value(nutrient_facts, 'phosphorus')
        }

        return data


@task(task_id="transform")
def structure_all_foods_data(path:str):

    foods_data = _read_file(path)

    transformed_foods_data_list = []

    for food in foods_data:
        data = structure_food_data(food['item'])
        if data != None:
            transformed_foods_data_list.append(data)
    
    file_name = path.split('/')[-1]
    transformed_path = _save_to_file(file_name, transformed_foods_data_list)
    
    return transformed_path


if __name__ == '__main__':
    print(structure_all_foods_data(fetch_all_food_data()))
