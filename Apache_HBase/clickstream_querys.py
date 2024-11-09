import happybase 
import pandas as pd 
from datetime import datetime
from itertools import *
from tabulate import tabulate
import argparse
import sys

"""
-Implementar la base de datos de columnas con Apache Hbase con toda la configuacion lo mejr posible con lo siguiente:
-se declara una varible file_path_csv para leer los datos con la libreria pandas usando el mismo archivo que se
trabajo en la fase 3 de este proyecto big Data(CSV)
-se crea una estructura con en nombre de la tabla (click_stream) y de da la estructura para la creaciin de la DB
donde se compone con Grupos de familias, donde cada columna del grupo va asociada al grupo como se muestra:
- families: familia de grupos
- column_families: los colummnas de cada grupo de familias.
- Se utilizaron librerias tabulate, pandas, itertools y argparse para el script
- Ejecucion del script -> ::
- py clickstream_querys.py --help | -h => muestra los camandos y que hace cada uno
- py clickstream_querys.py -c | --connect => muestra la conexion si es exitoda en caso de no Error
- py clickstream_querys.py -d | --delete => Elimina la base de datos actual (click_stream), para despues crear los datos
- py clickstream_querys.py => se ejecuta normal y muestra los resultados.
Documentacion online de que me base para crear el proyecto::
- https://realpython.com/python-exceptions/ -> como trabajar con exceptions
- https://happybase.readthedocs.io/en/latest/user.html -> uso de la guia de usuario en happybase
- https://happybase.readthedocs.io/en/latest/api.html -> uso de los metodos con todos sus params en happybase
- https://realpython.com/python-command-line-arguments/ -> como trabajar con la linea de comandos
- https://pypi.org/project/tabulate/ -> como trabajar con la vista de las tablas en la terminal
- https://www.tutorialspoint.com/hbase/index.htm -> explicacion de la BD de columnas
"""

file_path_csv = "ckickstream_blognews_05.csv"
table_name = 'click_stream' 
families = { 
    'user_data': dict(),
    'page_data': dict(),
    'device_data': dict(),
    'content_data': dict(),
    'geo_data': dict()
} 
column_families = {
    'user_data': [
        'User_ID',
        'Session_ID',
        'Timestamp'
    ],
    'page_data': [
        'Page_URL',
        'Time_Spent_seconds',
        'Clicks'
    ],
    'device_data': [
        'Browser_Type', 
        'Device_Type'
    ],
    'content_data': [
        'Article_Category',
        'Article_Name',
        'Interacted_Elements',
        'Video_Views'
    ],
    'geo_data': ['Geo_Location']
}
type_tablefmt = "pretty"

# fn crear la conexion a la base de datos Apache Hbase  
def connection_hbase():
    try:
        connection = happybase.Connection('localhost')  # Cambia 'localhost' por la dirección de tu servidor HBase
        connection.open() # se abre una conexion puente
        print("Conexión establecida con HBase")
        return connection
    except Exception as e:
        print(f"Error al conectar con HBase: {str(e)}")
        sys.exit(1)
        
# Crear o eliminar la tabla en HBase
def create_table(connection, table_name, families, delete_table = False):
    tables = map(lambda x : x.decode(), connection.tables()) #docodificamos los string ya que estan en bytes a str
    if delete_table and table_name in tables: # si valida accion eliminar tabla y existe la table la elimina la tabla de la DB
        connection.delete_table(table_name, disable = True) # elimina la table
        print(f"Eliminando tabla existente - {table_name}")
        return # sale de la funcion

    if table_name not in tables: # si la tabla no esta es la DB, la crea
        connection.create_table(table_name, families)
        print(f"Tabla {table_name}, creada con las familias de columnas: {list(families.keys())}")
    else: # si no imprime la tabla acualmente existente
        print(f"La tabla {table_name}, existe en la BD.")
        
#fn para generar los datos en el diccionario en cada fila de datos
def generate_data_dict(row, column_families, column_aliases = None):
    """
    params:
    row: CSV de datos de cada fila -> (list),
    column_families: la familia de columnas en una coleccion de cada columnas establecida al crear la tabla -> (dict)
    column_aliases: Grupo de valores, clave es cada columna de datos csv con el valor que se a guardar para el alias -> (dict)
    """
    data_dict = {}
    
    if column_aliases is not None: # Valid column_aliases es un dict
        if not isinstance(column_aliases, dict): # si no lo es lanza error
            raise ValueError("column_aliases debe ser un diccionario.")
        
        if not column_aliases:  # si esta vacio lanza error
            raise ValueError("column_aliases no puede estar vacío.")
    
    for family, columns_data in column_families.items(): # me retorna cada familia por el grupo de datos que se van a guardar
        for colum_data in columns_data: # iter list para devolver el valor de cada grupo de famila
            # si se pasa un alias para cada columna del grupo de familias se usa ese, sino default value
            """
            si tengo esta dict con los siguientes alias se usan esos usando (v) ejemplo::
            column_aliases = {
                'User_ID': 'User_temp_Id', 
                'Session_ID': 'Storage_Id', 
            }
            """
            alias = column_aliases.get(colum_data, f"Valor no encontrado: {str(colum_data)}") if column_aliases else colum_data
            """
            se guarda el un dict, donde la (k => user_data:User_ID) para obtener y guardar los datos.
            y el (v => valos de los datos del archivo CSV por cada [fila & columna]) del dict
            """
            data_dict[f"{family}:{alias}"] = str(row[colum_data])
    return data_dict

# Funcion para guarar los datos en HBase desde el CSV
def add_data_hbase(connection, table_name):
    table = connection.table(table_name) # conectamos la tabla ya creada
    is_data_table = list(table.scan(limit = 1)) # verificamos si hay datos y los limitamos a (1)

    if not is_data_table: # si no hay datos los agrega
        data = pd.read_csv(file_path_csv, encoding='utf-8') # leemos el archivo csv y lo iteramos
        # Agregar datos a HBase
        for index, row in data.iterrows(): 
            #se debe crear un indice para cada Fila de datos guardados, usando todos los grupos de familias 
            table.put(f'user_{str(index)}', generate_data_dict(row, { # var row, con la list de datos de cada fila CSV 
                'user_data': ['User_ID', 'Session_ID', 'Timestamp'],
                'page_data': ['Page_URL', 'Time_Spent_seconds', 'Clicks'],
                'device_data': ['Browser_Type', 'Device_Type'],
                'content_data': ['Article_Category', 'Article_Name', 'Interacted_Elements', 'Video_Views'],
                'geo_data': ['Geo_Location']
            }))

        print("Datos guardados exitosamente.") # imprime datos
    else:
        print("La tabla ya contiene datos.") # imprime ya exiten datos
    
    return table

def result_data_hbase(table_data):
    """
    uso la libreria tabulate para una mejor visualizacion de los datos.
    """
    print("********** Mostrando resultados: **********")
    #***********************************| 1. Numero total de usuarios |***********************************#
    total_users = set() # creamos un set objeto, porque las claves son Unicas, usando valores distintos
    for key, row in table_data.scan():
        # Extraer y procesar campos necesarios de cada fila
        user_id = row.get(b'user_data:User_ID', '').decode() # si no retorna, dato pasamos vacio ""
        total_users.add(user_id) # agregamos los daros al set
    
    total_users = str(len(total_users))
    print("\n1. Numero total de usuarios:\n")
    print(tabulate([[total_users]], headers = ["Numero total de usuarios"], tablefmt = type_tablefmt))
    
    #***********************************| 2. Sesiones por dispositivo |***********************************#
    # Extraer y ordenar todos los dispositivos de forma asendente por Device_Type
    device_sessions = sorted([
        [row[b'device_data:Device_Type'].decode()] 
        for key, row in table_data.scan()
    ])

    # necesitamos agrupar los datos por tipo de dispositivo ya que tienen valores duplicados y luego contarlos
    # creamos una diccionarioo con el tipo de device y al cantidad de sessiones
    sessions_by_device = [
        [device, len(list(group))] 
        for device, group in groupby(device_sessions)
    ]
    
    # imprimo los datos
    print("\n2. Sesiones por dispositivo:\n")
    print(tabulate(sessions_by_device, headers = ["Tipo de dispositivo", "Cantidad de sessiones"], tablefmt = type_tablefmt))
    
    #***********************************| 3. Numero total de cliks por usuario |***********************************#
    user_clicks = sorted(
        [
            [row[b'user_data:User_ID'].decode(), int(row[b'page_data:Clicks'].decode())] 
            for key, row in table_data.scan()
        ],
        key = lambda x : x[0] # lo ordeno por user id de forma asedente
    )
    
    # creo una matriz con los datos agrupando por user_id y luego sumo todos los click de cada usuario
    clicks_per_user = [
        [user_id, sum(clicks for _, clicks in group)] 
        for user_id, group in groupby(user_clicks, key = lambda x : x[0])
    ]
    
    # imprimo los datos
    print("\n3. Numero total de cliks por usuario\n")
    print(tabulate(clicks_per_user, headers = ["User Id", "Numero Total de click"], tablefmt = type_tablefmt))
    
    #***********************************| 4. Numero total de cliks por Articulo |***********************************#
    article_clicks = sorted(
        [
            [row[b'content_data:Article_Name'].decode(), int(row[b'page_data:Clicks'].decode())]
            for key, row in table_data.scan()
        ],
        key = lambda x : x[0]
    )
    
    # decodifico los datos a utf-8 para mostrarlos correctamente en la tabla son decoficicarlos
    clicks_by_article = [
        [article_name.encode('latin1').decode('utf-8'), sum(clicks for _, clicks in group)]
        for article_name, group in groupby(article_clicks, key = lambda x : x[0])
    ]
    
    # imprimo los datos
    print("\n4. Numero total de cliks por Articulo\n")
    print(tabulate(clicks_by_article, headers = ["Nombre Articulo", "Numero Total de click"], tablefmt = type_tablefmt))
    
    #***********************************| 5. Tiempo promedio que los usuarios pasan en las paginas |***********************************#
    # ordenos los resultados creando una matriz para la tabla por user_Id para agruparlos
    user_time = sorted(
        [
            [row[b'user_data:User_ID'].decode(), int(row[b'page_data:Time_Spent_seconds'])]
            for key, row in table_data.scan()
        ],
        key = lambda x : x[0]
    ) 
    
    #agrupo los datos ya ordenaos por user_id
    grouped_user = groupby(user_time, key = lambda x : x[0])
    
    average_time_data = [] # Matrix data
    for user_id, group in grouped_user:
        group_list = list(group)
        if group_list: # lista no puede ser vacida, para no error divide_by_zero 
            total_time = sum(time_spent for _, time_spent in group_list) # sumo los datos 
            average_time = total_time / len(group_list) # divido los datos por la misma cantidad
            average_time_data.append([user_id, average_time]) # lso agrego al list
    
    # Ordenar los resultados por el tiempo promedio de forma descendente, [1] tiempo promedio
    average_time_user = sorted(average_time_data, key = lambda x: x[1], reverse = True)
    
    # imprimo los datos
    print("\n5. Tiempo promedio que los usuarios pasan en las paginas\n")
    print(tabulate(average_time_user, headers = ["Id del Usuario", "Tiempo Promedio"], tablefmt = type_tablefmt))
    
    #***********************************| 6. Obtener la primera y ultima interacción por usuario en 2024 |***********************************#
    # ordenos los resultados creando una matriz para la tabla por user_Id para agruparlos
    user_timestamp = sorted(
        [
            [row[b'user_data:User_ID'].decode(), row[b'user_data:Timestamp'].decode()]
            for key, row in table_data.scan()
        ],
        key = lambda x : x[0]
    ) 
    
    # Filtra año 2024
    user_timestamp = [
        [user_id, timestamp] for user_id, timestamp in user_timestamp
        if datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').year == 2024
    ]

    #agrupo los datos ya ordenaos por user_id
    grouped_user = groupby(user_timestamp, key = lambda x : x[0])
    
    # Calcular la primera y ultima fecha por iteracion por usurr
    user_timestamp_data = []
    for user_id, group in grouped_user:
        group_list = list(group)
        if group_list: # lista no puede ser vacida
            # Convertir los timestamps de string a datetime para comparar
            # creamos un array y obtyenenor todos datos para poder despues filtrarlos
            timestamps = [datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') for _, timestamp in group_list]
            data_first = min(timestamps) # la menor fecha 
            data_last = max(timestamps) # la mayor fecha
            # guardamos los datos de cada usuario, [user id, fecha inicio, fecha final]
            user_timestamp_data.append([user_id, data_first, data_last]) 

    # Ordenar los resultados por fecha memnor de cada ussuario
    user_interactions_2024 = sorted(user_timestamp_data, key = lambda x: x[1])
    
    # imprimo los datos
    print("\n6. Obtener la primera y ultima interacción por usuario en 2024 \n")
    print(tabulate(user_interactions_2024, headers = ["Id del Usuario", "Fecha Inicio", "Fecha Final"], tablefmt = type_tablefmt))
    
#fn ejecuta todo el code
def run():
    try:
        #agrego la descripcion de los argumentos, cuando pasa -h | --help
        parser = argparse.ArgumentParser(description="Apache HBase, --delete & --connect")
        
        # parametros permitidos (--delete & --connect), no toma valor y por defecto es (True) si lo pasa
        parser.add_argument('-d', '--delete', action='store_true', help=f"Elimina la tabla de la DB: {table_name}")
        parser.add_argument('-c', '--connect', action='store_true', help="Realiza una prueba de la conexion a la DB")
        
        # Parseo de argumentos
        args, unknown_args = parser.parse_known_args()
        
        # Verifica si hay parametros no establecidos
        if unknown_args: raise ValueError(f"Error: Se han pasado argumentos no permitidos: {unknown_args}")
        
        if args.connect: # si pasa el argv (-c | --connect)
            print(f"Validando la conexion: {args.connect}")
            connection_hbase()
            return
        
        if args.delete: # si pasa el argv (-d | --delete)
            print(f"Eliminando la tabla de la DB: {args.delete}")
            # Conectar y crear la tabla de la base de datos
            create_table(connection_hbase(), table_name, families, args.delete)
            return
        
        # si no proporciona ninguno de los parametro: Conectar y crear la tabla de la base de datos
        connection = connection_hbase()
        create_table(connection, table_name, families)
        
        # guardar los datos en la BD
        table_data = add_data_hbase(connection, table_name)
        
        # visualizar los datos, usando consultas y analisis de datos
        result_data_hbase(table_data)
        
    except Exception as e:
        print("Error:", e)
        sys.exit(1)
    except ValueError as e:
        print("Error:", e)
        print("Generate generate_data_dict() function wasn't executed.")
        sys.exit(1)
    finally:
        # valida en caso de que la conexion de error y la variable connection (local) de la fn, no esta definica
        if 'connection' in locals():
            connection.close() # cierra la conexion  
            print("Conexión con HBase cerrada.")

if __name__ == "__main__":
    run()
    
