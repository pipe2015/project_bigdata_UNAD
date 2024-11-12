from conection.connec import connect_database
from generate_DBdata.generate_data import add_table_users, add_table_categories_articles, add_table_sessions, add_table_pageViews
import Consultas.querys as consultas
import os
import sys

# create current directory (es para obtener la ruta principal del proyecto)
current_directory = os.path.dirname(os.path.abspath(__file__))


def run():
    try:
        #creamos la conexion 
        connect_database()
        
        #creando tablas y registros necesarios para la base de datos 
        users = add_table_users() # generamos los primeros registros para la BD
        add_table_categories_articles(current_directory) # generamos los categorias por articulos
        all_users_sessions = add_table_sessions(users) # generalos las 100 sessiones por usuario
        add_table_pageViews(all_users_sessions) # geenramos los 100 vistas por pagina de cada session por cliente
        
        #realizar consultas: consultas basicas (selects, updates, delete, filtros y operadores) Datos DB
        consultas.all_users()
        consultas.all_category_article()
        consultas.get_sessions_oneuser("Felipe Arias")
        consultas.get_pageurls("Felipe Arias", "Desktop")
        consultas.update_user("Felipe Arias", "crossdev")
        consultas.delete_user("crossdev")
        
        #realizar consultas: consultas (Contar, sumar, promedio) Datos DB
        consultas.count_all_users()
        consultas.sum_time_page_category("Tecnologia")
        consultas.avg_time_all_suers()
        
        
    except Exception as e:
        print("Error:", e)
        sys.exit(1)

#ejecutamos la app
if __name__ == "__main__":
    run()