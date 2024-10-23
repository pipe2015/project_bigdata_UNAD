"importamos las dependencias necesarias para trabajar"
import json
import random
import os

MAX_NUM_CATEGORIES = 7 # Definir el numero maximo de categorias disponibles en el archivo

file_name = "articles_categories" # file name archivo json alojado

# create current directory (es para obtener la ruta principal del proyecto)
current_directory = os.path.dirname(os.path.abspath(__file__))

# join path (un la ruta de acuerdo al os que este ejecutando)
file_path = os.path.join(current_directory, file_name + ".json")

with open(file_path, 'r') as json_file: data = json.load(json_file) # Leer el archivo JSON

def get_list_category():
    return list(data.keys())

#las siguientes funnciones con necesarias para exportar el csv que vamos a utilizar
def get_random_category(): # Obtener una key (categoría) aleatoria sin parámetros en la función
    return random.choice(get_list_category())

def get_articles_by_category(category): # Obtener los datos de una categoría por parámetro params: (category => str)
    category = category.lower()
    keys_normalized = {key.lower(): value for key, value in data.items()} # insensible serach key mayus | minus
    return keys_normalized.get(category, "Categoría no encontrada")

def get_random_category_and_articles(): # Obtener una key aleatoria y retornar la categoría + los datos
    random_category = get_random_category()
    return (random_category, data[random_category])

# test json data
"""
random_category = get_random_category()
print(f"Categoria aleatoria: {random_category}")

category = 'politica'
articles = get_articles_by_category(category)
print(f"\nArticulos en la categoría '{category}':")
for article in articles:
    print("-", article)


random_category, articles = get_random_category_and_articles()
print(f"\nCategoria aleatoria y sus articulos: {random_category}")
for article in articles:
    print("-", article)

"""

