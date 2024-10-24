from load_data_json import * # importamos el archivo json
#importamos depenedencias
import pandas as pd
import random
import re
import uuid
from datetime import datetime, timedelta
from multiprocessing import Pool
# process bar 0 - 100% para mejor presentacion lib: https://tqdm.github.io/
from tqdm import tqdm

"""
Problema con esta imolementacion esque si por ejemplo, renderizo 500_000 registros se demora,
entonces boy a implementar el modulo multiprocessing nativo de Py para dividir la generacion de registros
dicen (Divida y venceras) eso es lo vamos a implementar:
La idea es procesar Resistros en (paralelo) o sea al mismo tiempo, entonces si tengo 150_000 registros voy 
a crear 5 archivos temporales en cache (150_000 / 5) = 30_000 registros al mismo tiempo y luego los uno.
si quieren saber mas: docs.
https://docs.python.org/3/library/multiprocessing.html,
https://www.digitalocean.com/community/tutorials/python-multiprocessing-example
Esta es mi pagina favorita: (realpython.com).
https://realpython.com/courses/speed-python-concurrency/
"""

num_records = 150_000  # count registros
num_users = 5  # Numero de usuarios únicos
file_name_csv = "ckickstream_blognews"
num_processes = 5  # Número de procesos paralelos
records_per_process = num_records // num_processes  # Registros por proceso, floor decimal minimo

# Definir los 5 usuarios
users = [
    {"User_ID": "U001"},
    {"User_ID": "U002"},
    {"User_ID": "U003"},
    {"User_ID": "U004"},
    {"User_ID": "U005"},
]

# Definir las variables para personalización de la experiencia del usuario en clickstream
fields_data = {
    'User_ID': users, 
    'Session_ID': -1, 
    'Timestamp': None, 
    'Page_URL': 'https://crossdevblog.com/article', 
    'Time_Spent_seconds' : None, 
    'Clicks' : -1, 
    'Browser_Type' : ['Chrome', 'Firefox', 'Safari', 'Edge'],
    'Device_Type': ['Desktop', 'Mobile', 'Tablet'],  
    'Article_Category': get_list_category(),
    'Article_Name': None, 
    'Interacted_Elements': ['buttons', 'links', 'videos', 'images', 'forms', 'multimedia'], 
    'Video_Views': None, 
    'Geo_Location': ['USA, New York', 'UK, London', 'Canada, Toronto', 'Australia, Sydney', 'Germany, Berlin', 'Colombia, Bogota']
}

# Funciones auxiliares
def random_timestamp_one_year():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_time = timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    return start_date + timedelta(days=random_days) + random_time

# Generar 100 registros para los 5 usuarios
def list_generate_Data_csv(num_records):
    data = []
    for i in range(num_records):
        user = random.choice(users)
        user_id = user["User_ID"]
        session_id = str(uuid.uuid4())[:20]
        timestamp = random_timestamp_one_year().strftime('%Y-%m-%d %H:%M:%S')
        page_url = f"{fields_data.get('Page_URL')}/{random.randint(1, 100)}"
        clicks = random.randint(1, 20)
        time_spent_seconds = random.randint(10, 600)
        browser_type = random.choice(fields_data.get('Browser_Type'))
        device_type = random.choice(fields_data.get('Device_Type'))
        article_category = random.choice(fields_data.get('Article_Category'))
        article_name = random.choice(get_articles_by_category(article_category))
        video_views = random.randint(0, 5)
        interacted_elements = random.choice(fields_data.get('Interacted_Elements'))
        geo_location = random.choice(fields_data.get('Geo_Location'))
        
        # Agregar el registro a la lista de datos
        data.append([
            user_id, session_id, timestamp, page_url, clicks, time_spent_seconds, browser_type, device_type,
            article_category, article_name, video_views, interacted_elements, geo_location
        ])
    return data

def get_file_name_ext(filename, concat = ""):
    filename = filename.split("/")
    name = filename[1].split(".")[0]
    ext = "." + filename[1].split(".")[1]    
    return filename[0] + "/" + name + concat + ext

def get_file_name(filename):
    return filename.split("/")[1].split(".")[0]

def convert_file_size(size_in_bytes):
    if size_in_bytes < 1024: return f"{size_in_bytes} Bytes"
    elif size_in_bytes < 1024 ** 2: return f"{size_in_bytes / 1024:.2f} KB"
    elif size_in_bytes < 1024 ** 3: return f"{size_in_bytes / (1024 ** 2):.2f} MB"
    else: return f"{size_in_bytes / (1024 ** 3):.2f} GB"

def get_file_info(file_path):
    # Obtener la fecha de creación del archivo
    creation_time = os.path.getctime(file_path)
    creation_date = datetime.datetime.fromtimestamp(creation_time)

    # Convertir el tamaño a una unidad apropiada
    formatted_size = convert_file_size(os.path.getsize(file_path))

    print(f"Fecha de creación: {creation_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Tamaño del archivo: {formatted_size}")

def generate_csv(filename = "", num_records = -1):
    global list_generate_Data_csv
    list_generate_Data_csv = list_generate_Data_csv(num_records)
    df_Dataframe = pd.DataFrame(list_generate_Data_csv, columns = list(fields_data.keys()))
    # Verificar si el archivo ya existe
    if os.path.exists(filename):
        files = os.listdir("List_csv_data")
        
        # Filtrar los archivos que comienzan con file y terminan con '.csv'
        user_behavior_files = [f for f in files if f.startswith(get_file_name(filename)) and f.endswith('.csv')]
        
        numbers = re.findall(r'\d+', user_behavior_files[len(user_behavior_files) - 1])
        numbers = [int(num) for num in numbers]
        
        if len(numbers) > 0:
            index = "_0" + str(numbers[0] + 1)
            df_Dataframe.to_csv(get_file_name_ext(filename, index), index=False, encoding='utf-8')
        else:
            df_Dataframe.to_csv(get_file_name_ext(filename, "_01"), index=False, encoding='utf-8')
    else:
        # Crear DataFrame con las fechas aleatorias
        df_Dataframe = pd.DataFrame(list_generate_Data_csv, columns = list(fields_data.keys()))
        df_Dataframe.to_csv(filename, index=False, encoding='utf-8')
    
    return df_Dataframe

#///////////////////////////////////////////** Implementando processing Parallel **/////////////////////////////////////////

# fn que genera los registros en paralelo por records_per_process
def generate_process_data_parallet(pct_processs_idx): # params => pct_processs_idx = Int
    data = []
    for i in range(num_records):
        user = random.choice(users)
        user_id = user["User_ID"]
        session_id = str(uuid.uuid4())[:20]
        timestamp = random_timestamp_one_year().strftime('%Y-%m-%d %H:%M:%S')
        page_url = f"{fields_data.get('Page_URL')}/{random.randint(1, 100)}"
        clicks = random.randint(1, 20)
        time_spent_seconds = random.randint(10, 600)
        browser_type = random.choice(fields_data.get('Browser_Type'))
        device_type = random.choice(fields_data.get('Device_Type'))
        article_category = random.choice(fields_data.get('Article_Category'))
        article_name = random.choice(get_articles_by_category(article_category))
        video_views = random.randint(0, 5)
        interacted_elements = random.choice(fields_data.get('Interacted_Elements'))
        geo_location = random.choice(fields_data.get('Geo_Location'))
        
        # Agregar el registro a la lista de datos
        data.append([
            user_id, session_id, timestamp, page_url, clicks, time_spent_seconds, browser_type, device_type,
            article_category, article_name, video_views, interacted_elements, geo_location
        ])
    
    # Guardar los datos temporales en cache osea pueede ser en un archivo en local
    temporary_file_name = f"{file_name_csv}_{pct_processs_idx}.csv"    
    df_Dataframe = pd.DataFrame(data, columns = list(fields_data.keys()))
    df_Dataframe.to_csv(temporary_file_name, index=False, encoding='utf-8')
    return temporary_file_name

# fn para combinar los archivos en uno solo en la carperta
def combine_csv_files(file_list, output_file):
    df_list = [pd.read_csv(file) for file in file_list]
    combine_df = pd.concat(df_list, ignore_index=True)
    combine_df.to_csv(output_file, index=False, encoding='utf-8')

# Función principal para ejecutar el procesamiento en paralelo
def run_generate_csv():
    with Pool(num_processes) as pool: # creamos una instance del pool para trabajar con datos en proceso
        # ejecutamos y creamos una lista de los archivos temp, lo que hago es mapiarlos y retorna la urs temp fil y lsito. 
        file_list = pool.map(generate_process_data_parallet, range(num_processes)) # y pasados un arreglo de los idx = 1 - 5.
    
    # Combinar los archivos CSV generados en un solo archivo
    combine_csv_files(file_list, "List_csv_data/test_processing_file.csv")

    # Eliminar los archivos temporales
    for file in file_list:
        os.remove(file)


# auto eject fn main
if __name__ == "__main__":
    print("generando::=")
    run_generate_csv()

"""
file_name_csv = "List_csv_data" + "/" + file_name_csv + ".csv" 
df_Dataframe = generate_csv(file_name_csv, num_records)
"""

# Funciones de análisis
def get_average_time_spent(df):
    return df['Time_Spent_seconds'].mean()

def get_clicks_distribution(df):
    return df['Clicks'].value_counts()

def get_articles_by_category(df):
    return df['Article_Category'].value_counts()

def get_geo_distribution(df):
    return df['Geo_Location'].value_counts()

# Ejemplo de consultas
average_time_spent = get_average_time_spent(df_Dataframe)
clicks_distribution = get_clicks_distribution(df_Dataframe)
articles_by_category = get_articles_by_category(df_Dataframe)
geo_distribution = get_geo_distribution(df_Dataframe)

# Mostrar resultados
print("Average Time Spent:", average_time_spent)
print("Clicks Distribution:\n", clicks_distribution)
print("Articles by Category:\n", articles_by_category)
print("Geographic Distribution:\n", geo_distribution)
