from conection.connec import connect_database
from generate_DBdata.generate_data import add_table_users, add_table_categories_articles, add_table_sessions, add_table_pageViews
import os
import sys

# create current directory (es para obtener la ruta principal del proyecto)
current_directory = os.path.dirname(os.path.abspath(__file__))


def run():
    #try:
        #creamos la conexion 
        connect_database()
        
        users = add_table_users() # generamos los primeros registros para la BD
        add_table_categories_articles(current_directory) # generamos los categorias por articulos
        all_users_sessions = add_table_sessions(users) # generalos las 100 sessiones por usuario
        add_table_pageViews(all_users_sessions) # geenramos los 100 vistas por pagina de cada session por cliente
        
        
        
    #except Exception as e:
    #    print("Error:", e)
    #    sys.exit(1)

#ejecutamos la app
if __name__ == "__main__":
    run()