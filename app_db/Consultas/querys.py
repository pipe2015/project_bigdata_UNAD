# importamos las modelos, para trabajar con ellos 
from models.users import User
from models.sessions import Session
from models.categories import Category
from models.articles import Article
from models.page_views import PageView
from datetime import datetime
from tabulate import tabulate
import random # libreria para crear datos aleatorios
import json
import os
import uuid
import math

#definir variables necesarias
type_tablefmt = "pretty"

#******************************| consultas basicas (selects, updates, delete, filtros y operadores) Datos DB |******************************

"""
# 1. Seleccionar todos los usuarios:
Usando Monngo Shell (JS):
db.user.find({})
"""
def all_users():
    users = User.objects() # obtengo la tabla y llamo todos los objets
    users = [ # creo la matriz de datos para usarlo para tabulate
        [user.id, user.name, user.geo_location] 
        for user in users
    ]
    # imprimo los datos
    print("\n1. Seleccionar todos los usuarios:\n")
    print(tabulate(users, headers = ["Object(Id) de Usuario", "Nombre del Usuario", "Lugar"], tablefmt = type_tablefmt))

"""
# 2. Seleccionar todas las categorias por articulos
Usando Monngo Shell (JS):
db.category.aggregate([
    {
        $lookup: {
            from: "article", // Colecccion con la que se hace la relacion
            localField: "_id", // Campo en Category que se usara para la relación
            foreignField: "category", // Campo en Article que se usara para la relación
            as: "articles" // Nombre del array de resultados
        }
    },
    {
        $project: { //pasamos los datos retornados por la $lookup en as
            category_name: 1, // pasamos el dato, nombre de la categoria
            articles: { 
                /* modificamos los datos con la funcion $map le pasamos el array (articles) lo nombramos con un alias(article)
                y retornamos los articulos por cada categoria 
                */
                $map: { input: "$articles", as: "article", in: "$$article.article_name" } 
            }
        }
    }
]).forEach(result => {
    articles = result.articles.reduce((str_concat, v) => `${str_concat}${v}\n`, '')
    print(`Category: ${result.category_name}, Articles: ${articles}`);
});
"""
def all_category_article():
    categories = Category.objects() # llamo al objeros de las categorias
    mt_categoryArticles = [] # creo una matriz de datos para la tabla
    for category in categories:
        articles = Article.objects(category = category) # relaciono 1 - *, por el id (category | category.id)
        article_names = [article.article_name for article in articles]
        # Agregar a la tabla: nombre de la categoria y los nombres de los artículos combinados
        mt_categoryArticles.append([category.category_name, ", ".join(article_names)])

    # imprimo los datos
    print("\n2. Seleccionar todas las categorias por articulos\n")
    print(tabulate(mt_categoryArticles, headers = ["Category", "Articles"], tablefmt = type_tablefmt))
    
"""
# 3. Seleccionar todas la sessiones por un usuario de nombre
Usando Monngo Shell (JS):
var get_sessions_oneuser = user_name => {
    //imprimimos el valos de los datos
    print(`\n3. Sesiones para el usuario '${user_name}':\n`);

    return db.user.aggregate([
        {
            $match: { name: user_name } // Filtra el usuario por nombre
        },
        {
            $lookup: {
                from: "session",         // Colección relacionada
                localField: "_id",        // Campo en User (clave primaria)
                foreignField: "user",     // Campo en Session que referencia al usuario
                as: "sessions"            // Nombre del campo donde se guardarán las sesiones
            }
        },
        { $unwind: "$sessions" },          // separamos los datos del array y los obtenemos en un solo documento cada uno (json)
        {
            $project: {
                "sessions.session_id": 1, // require 
                "sessions.timestamp": 1, // require 
                "sessions.browser_type": 1, // require 
                "sessions.device_type": 1 // require 
            }
        }
    ]).map(result => {
        let session = result.sessions;
        return `Session ID: ${session.session_id}, Tiempo de Session: ${session.timestamp}, Navegador: ${session.browser_type}, Dispositivo: ${session.device_type}`
    });
}
get_sessions_oneuser('David Lopez').pretty()
"""
def get_sessions_oneuser(user_name):
    user = User.objects(name = user_name).first() # Buscar el usuario por nombre y obtenemos el primer registro 
    
    if not user:
        print("usuario no encontrado.")
        return

    mt_session_user = []
    for session in Session.objects(user = user): # relaciono 1 - *, por el usuario filtrado
        mt_session_user.append([
            session.session_id,
            session.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            session.browser_type,
            session.device_type
        ])
    
    # imprimo los datos
    print(f"\n3. Sesiones para el usuario '{user.name}':\n")
    print(tabulate(mt_session_user, headers = [
        "Session ID", 
        "Tiempo de Session", 
        "Navegador", 
        "Dispositivo"
    ], tablefmt = type_tablefmt))

"""
# 4. Seleccionar todas las direcciones de paginas url, por nombre de usuario y dispositivo
Usando Monngo Shell (JS):
var get_pageurls = (user_name, device_type) =>  {
    let user = db.user.findOne({ name: user_name }); // obtenemos los datos del usuario por nombre
    if (!user) {
        print("Usuario no encontrado.");
        return;
    }
    
    print(`\n4. Paginas vistas por '${user.name}' en el dispositivo '${device_type}':\n`);
    return db.page_view.aggregate([
        {
            $lookup: {
                from: "session",
                localField: "session",
                foreignField: "_id",
                as: "session_info"
            }
        },
        { $unwind: "$session_info" },
        {
            $match: {
                "session_info.user": user._id,
                "session_info.device_type": device_type
            }
        },
        {
            $project: {
                page_url: 1,
                "session_info.timestamp": 1,
                "session_info.browser_type": 1,
                "session_info.device_type": 1
            }
        }
    ]).map(page_view => {
        return `Pagina URL: ${page_view.page_url}, Tiempo de Session: ${page_view.session_info.timestamp}, Navegador: ${page_view.session_info.browser_type}, Dispositivo: ${page_view.session_info.device_type}`;
    });
}
get_pageurls('David Lopez', 'Desktop').pretty();
"""
def get_pageurls(user_name, device_type):
    user = User.objects(name = user_name).first() # Buscar el usuario por nombre y obtenemos el primer registro 
    
    if not user:
        print("Usuario no encontrado.")
        return

    sessions = Session.objects(user = user, device_type = device_type) # relaciono 1 - *, por el usuario filtrado
    page_views = PageView.objects(session__in = sessions) # se usa el operador (in) de mongo, par realizar la lista de los valores por sessiones

    mt_data = []
    for page_view in page_views:
        mt_data.append([
            page_view.page_url,
            page_view.session.timestamp.strftime("%Y-%m-%d %H:%M:%S"), # ya que los datos tiene la relacion los puedo obtener
            page_view.session.browser_type,
            page_view.session.device_type
        ])
    
    # imprimo los datos
    print(f"\n4. Paginas vistas por '{user.name}' en el dispositivo '{device_type}':\n")
    print(tabulate(mt_data, headers=[
        "Pagina URL", 
        "Tiempo de Session", 
        "Navegador", 
        "Dispositivo"
    ], tablefmt = type_tablefmt))

"""
# 6. Actualizar el nombre de un usuario cualquiera
Usando Monngo Shell (JS):
var update_user = (name, new_name) => {
    let user = db.User.findOne({ name: name });
    if (!user) {
        print("Usuario no encontrado.");
        return;
    }
    db.User.updateOne({ name: name }, { $set: { name: new_name } }); // update user $set nuevo usuario
    let updated_user = db.User.findOne({ name: new_name }); // lo buscamos en la db, ya actualziado
    print(`\n5. Actualizar el nombre del usuario:\n`);
    print(`Nombre Actual: ${name}, Nombre Actualizado: ${updated_user.name}`);
}
update_user("nombre_actual", "nombre_nuevo"); // ejecutamos la funcion
"""
def update_user(name, new_name):
    user = User.objects(name = name).first() # Buscar el usuario por nombre y obtenemos el primer registro 
    
    if not user:
        print("Usuario no encontrado.")
        return
    
    # uso el objeto user y obtego un usuario
    User.objects(name = name).update(set__name = new_name) # uso el operador set de mongo y actualizo el objeto

    # muestro la actualizacion
    updated_user = User.objects(name = new_name).first() # obtengo otrazez, el mismo usuario actualizado por nombre
    
    # imprimo los datos
    print(f"\n5. Actualizar el nombre del usuario:\n")
    print(tabulate([[name, updated_user.name]], headers = [
        "Nombre Actual", 
        "Nombre Actualizado"
    ], tablefmt = type_tablefmt))

"""
# 6. Eliminar un usuario cualquiero por mombre
Usando Monngo Shell (JS):
delete_user = name => {
    db.user.deleteOne({ name: name }); // elimino el usuario seleccionado
    if (!db.user.findOne({ name: name })) { // si fue eliminado correctamente
        print(`Usuario ${name} eliminado correctamente`);
    }
    print("\nUsuarios actuales:\n"); // se muestra que si se elimino
    db.user.find({}).pretty();
}
"""
def delete_user(name):
    User.objects(name = name).delete() # busco el usuario y lo elimino

    if not User.objects(name = name):
        print(f"Usuario {name} eliminado correctamente")

    all_users() # verifico si el usuario de elimino

#******************************| consultas (Contar, sumar, promedio) Datos DB |******************************

# 1. Contar la cantidad de usuarios 
def count_all_users():
    user_count = User.objects.count() # llamo al objeto users y cuento con la funcion de agregacion
    
    # imprimo los datos
    print(f"\n6. Cantidad total de Usuarios:\n")
    print(tabulate([[str(user_count)]], headers = ["Numero Total de Usuarios"], tablefmt = type_tablefmt))

# 2. sumar el tiempo total de visualizaciones por categoria 
def sum_time_page_category(category_name):
    category = Category.objects(category_name = category_name).first() # Buscar el usuario por nombre y obtenemos el primer registro 
    
    if not category:
        print("Categoria no encontrado.")
        return
    
    # Sumar el tiempo total de visualizaciones en la tabla PageView
    total_time_spent = PageView.objects(category = category).sum("time_spent_seconds")
    
    # imprimo los datos
    print(f"\n7. Sumar el tiempo total de visualizaciones por categoria:\n")
    print(tabulate([[category_name, total_time_spent]], headers = [
        "Nombre de la Categoria",
        "Tiempo total en segundos"
    ], tablefmt = type_tablefmt))
    
# 3. obtener el promedio de visualizacion por usuario
def avg_time_all_suers():
    users = User.objects().all() # obtengo todos los usuarios

    mt_users_avg = [] # creo la matriz
    for user in users:
        one_user = Session.objects(user = user) # obtengo el usuario
        # usar el metodo (average) para calcular el promedio de time_spent_seconds
        avg_time = PageView.objects(session__in = one_user).average("time_spent_seconds")
        
        # si no se encuentran resultados, return 0
        if avg_time is None:
            avg_time = 0

        mt_users_avg.append([user.name, avg_time])

    # Mostrar los resultados usando tabulate
    print("\n8. Tiempo promedio de visualizacion por usuario:")
    print(tabulate(mt_users_avg, headers = ["Nombre de Usuario", "Tiempo Promedio en (segundos)"], tablefmt = type_tablefmt))