# importamos las modelos, para trabajar con ellos 
from models.users import User
from models.sessions import Session
from models.categories import Category
from models.articles import Article
from models.page_views import PageView
from datetime import datetime
import random # libreria para crear datos aleatorios
import json
import os
import uuid

file_name = "articles_categories" # file name archivo json alojado

# fn para insertar 5 usuarios
def add_table_users():
    #creamos 5 usuarios, para la base de datos
    users_data = [
        {"name": "Juan Pérez", "geo_location": "Mexico"},
        {"name": "Ana Gómez", "geo_location": "Argentina"},
        {"name": "Carlos Fernández", "geo_location": "España"},
        {"name": "Laura Rodríguez", "geo_location": "Colombia"},
        {"name": "David López", "geo_location": "Peru"},
    ]
    users = [User(**data).save() for data in users_data]
    return users

# fn para add categorias y articulos desde el JSON proporcionado
def add_table_categories_articles(current_directory):
    # join path (un la ruta de acuerdo al os que este ejecutando)
    file_path = os.path.join(current_directory, file_name + ".json")

    with open(file_path, 'r') as json_file: data = json.load(json_file) # Leer el archivo JSON
    
    # creo un diccionario a partir de los datos json
    categories_data = { category: list_articles for category, list_articles in data }
    
    # Insertar categorías
    categories = []
    for category_name in categories_data.keys():
        category = Category(category_name = category_name).save()
        categories.append(category)

    # Insertar artículos para cada categoría
    for category_name, articles in categories_data.items():
        category = Category.objects(category_name = category_name).first() # get el nombre de la categoria 
        for article_name in articles:
            Article(article_name = article_name, category = category).save()

    return categories

# fn add 100 sesiones por usuario
def add_table_sessions(users): # get tabla users, para generar los datos, relacion 1 a muchos por refs
    sessions = []
    for user in users:
        for i in range(100):  # 100 sesiones por usuario
            session = Session(
                session_id = str(uuid.uuid4())[:10],
                user = user,
                timestamp = datetime.now(),
                browser_type = random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                device_type = random.choice(["Desktop", "Mobile", "Tablet"])
            ).save()
            sessions.append(session)
    return sessions

# fn pagina de visitas, con los click la sessio del usuario, categoria y name articulo
def add_table_pageViews(sessions):
    for _ in range(100):
        category = random.choice(Category.objects())
        articles = random.choice(Article.objects(category = category))
        PageView(
            page_url = f"http://crossdev.com/{category.category_name.lower()}/{articles.article_name[:10]}",
            session = random.choice(sessions),
            time_spent_seconds = random.randint(10, 300),
            clicks = random.randint(1, 10),
            category = category,
            article = articles,
            interacted_elements = random.sample(["buttons", "videos", "links"], 2),
            video_views = random.randint(0, 5)
        ).save()