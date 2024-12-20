import time 
import json 
import random 
from kafka import KafkaProducer 

"""
en esta parte del codigo vamos a simualr la llegada de los datos en (Real time) pero necesitamos
usar el mismo modelo de datos que estamos trabajando para esto hay problemas:
1. necesitos importar el (.json) con las categorias y lsio articulos que usamos para poderlo simular
2. necesitamos crear el mismo modelo de datos, para esta ocasion como no estamos conectados a una api en una
pagina web o conexion websockest usaremos datos aleatorias creandos.
y eso es todo::.
3. usaremos y generamos en modelo aleatorio, asignandole el evento de envio (clickstream_data)
"""

send_event = "clickstream_data" # se puede cahnge
url_port = 'localhost:9092' # se define la ruta de excucha

# vamos a crear un diciionario con los datos del json (articles_categories) usados anteriomente.
datos_articles_categories = {
    "Politica": [
        "El impacto de las políticas comerciales globales", 
        "Reformas electorales: ¿Qué sigue?", 
        "Entendiendo la nueva ley de inmigración", 
        "Tensiones geopolíticas en el Medio Oriente",
        "El futuro de la diplomacia internacional",
        "Descifrando las nuevas leyes fiscales",
        "Desafíos de la democracia moderna",
        "La participación electoral y su importancia",
        "Política climática en acción",
        "El papel de las redes sociales en la política",
        "Líderes emergentes del mañana",
        "La nueva ola de nacionalismo",
        "Opinión pública y formulación de políticas",
        "Sanciones internacionales y guerras comerciales",
        "El papel de las mujeres en la política",
        "Una mirada a los movimientos políticos globales",
        "Enmiendas constitucionales: Pros y contras",
        "El futuro de las campañas políticas",
        "Debatiendo la política de salud",
        "Cómo la política moldea la economía global"
    ],
    "Tecnologia": [
        "El auge de la computación cuántica", 
        "IA: Dando forma al futuro", 
        "Blockchain más allá de las criptomonedas", 
        "Redes 5G: Lo que necesitas saber",
        "El papel de la ética en el desarrollo de IA",
        "Avances en biotecnología",
        "Autos autónomos: Una nueva era en el transporte",
        "Startups tecnológicas a seguir en 2024",
        "El futuro de la computación en la nube",
        "Amenazas de ciberseguridad en 2024",
        "La evolución de los smartphones",
        "Hogares inteligentes: ¿Conveniencia o riesgo de seguridad?",
        "Cómo la impresión 3D está revolucionando la fabricación",
        "Realidad aumentada en la vida cotidiana",
        "Robótica en el cuidado de la salud",
        "El Internet de las cosas: Conectando el mundo",
        "Exploración espacial: La nueva frontera tecnológica",
        "Avances en tecnología verde",
        "Privacidad de datos en la era digital",
        "Tecnología usable: La próxima gran cosa"
    ],
    "Deportes": [
        "Juegos Olímpicos 2024: Qué esperar", 
        "Estrellas emergentes en el fútbol", 
        "La evolución de los deportes electrónicos", 
        "Un vistazo a las estrategias de la NFL",
        "La ciencia detrás del rendimiento máximo",
        "Los 10 mejores momentos del tenis del año",
        "Rompiendo récords en atletismo",
        "El futuro del deporte femenino",
        "El negocio de los patrocinios deportivos",
        "Cómo la tecnología está cambiando el entrenamiento deportivo",
        "Detrás de las escenas de la Fórmula 1",
        "El auge de los deportes extremos",
        "Jugadores clave a seguir en la NBA",
        "Consejos de entrenamiento para maratones para principiantes",
        "El legado de los entrenadores deportivos famosos",
        "Cómo los deportes impactan la salud mental",
        "La popularidad global del cricket",
        "Dentro del mundo de la natación competitiva",
        "Analítica deportiva: El cambio de juego",
        "El papel de los aficionados en el éxito del equipo"
    ],
    "Entretenimiento": [
        "Las 10 mejores películas para ver en 2024", 
        "La evolución de las plataformas de streaming", 
        "Dentro del mundo de la música indie", 
        "Las estrellas más grandes de Hollywood en 2024",
        "El impacto de las redes sociales en las celebridades",
        "Festivales de música para ver este año",
        "El arte de contar historias en el cine",
        "Cómo han cambiado los programas de televisión en la última década",
        "El papel de la diversidad en el entretenimiento",
        "Los mejores documentales que debes ver",
        "Próximas producciones teatrales que no te puedes perder",
        "El auge del cine internacional",
        "Cómo los videojuegos se están convirtiendo en entretenimiento convencional",
        "El poder de las comunidades de fanáticos",
        "Predicciones de la temporada de premios 2024",
        "El futuro de la realidad virtual en el entretenimiento",
        "Guerra de streaming: Netflix vs. competidores",
        "Celebridades que usan su plataforma para el bien",
        "La influencia global del K-pop",
        "Reboots y remakes: La nueva tendencia de Hollywood"
    ],
    "Negocios": [
        "Las 10 mejores startups para seguir en 2024", 
        "El futuro del trabajo remoto", 
        "Cómo construir una marca fuerte", 
        "El impacto de la inflación en los mercados globales",
        "Prácticas empresariales sostenibles para 2024",
        "Tendencias de capital de riesgo a seguir",
        "Fusiones y adquisiciones en el sector tecnológico",
        "El futuro del comercio electrónico",
        "Por qué la educación financiera es clave para el éxito",
        "El papel de las mujeres en el liderazgo empresarial",
        "Cómo crear una estrategia empresarial ganadora",
        "El auge de la inversión socialmente responsable",
        "La economía de trabajos temporales: Pros y contras",
        "Navegando las regulaciones comerciales internacionales",
        "Cómo fomentar la innovación en el lugar de trabajo",
        "Transformación digital en industrias tradicionales",
        "La importancia del networking para emprendedores",
        "Cómo la IA está transformando el mundo empresarial",
        "Desafíos de la cadena de suministro global",
        "Crowdfunding: Una nueva era de financiamiento empresarial"
    ],
    "Salud": [
        "Los beneficios de una dieta basada en plantas", 
        "Conciencia sobre la salud mental en 2024", 
        "Los últimos avances en la investigación del cáncer", 
        "Cómo el sueño afecta el bienestar general",
        "El papel de la salud intestinal en la inmunidad",
        "Enfoques holísticos en la atención médica",
        "La ciencia de la longevidad",
        "Técnicas de mindfulness para reducir el estrés",
        "Rutinas de ejercicio para un corazón saludable",
        "Las principales tendencias de fitness en 2024",
        "Cómo manejar el dolor crónico",
        "El futuro de la telemedicina",
        "Comprendiendo la importancia de la hidratación",
        "La conexión entre la salud mental y física",
        "El auge de los rastreadores de fitness",
        "Cómo mantenerse saludable mientras viajas",
        "Los beneficios de los exámenes médicos regulares",
        "Manejo de la diabetes a través de la dieta",
        "Remedios naturales para dolencias comunes",
        "La importancia de la atención preventiva"
    ],
    "Ciencia": [
        "Explorando el océano profundo: Nuevos descubrimientos", 
        "Los últimos avances en la exploración espacial", 
        "Cambio climático: Cómo está afectando a la vida silvestre", 
        "Nuevos avances en energía renovable",
        "La ciencia detrás de los desastres naturales",
        "Cómo la actividad humana afecta a los ecosistemas",
        "El futuro de la edición genética",
        "Entendiendo la física cuántica",
        "El papel de las matemáticas en la vida cotidiana",
        "Avances en la tecnología médica",
        "La búsqueda de vida extraterrestre",
        "Cómo la IA está revolucionando la investigación científica",
        "Fuentes de energía renovable del futuro",
        "La importancia de la biodiversidad",
        "Descubrimientos en el campo de la paleontología",
        "Entendiendo el cerebro humano",
        "El papel de la física en la tecnología moderna",
        "Cómo el cambio climático está remodelando el planeta",
        "El debate ético en torno a la clonación",
        "Explorando los misterios de los agujeros negros"
    ]
}

# fn selecciona una claave y un valo para categoria y el atriculo de cada uno de los datos
def select_data_rand(list_data):
    rand_key = random.choice(list(list_data.keys())) # lo convirt en ua array para poser usar la rand choice
    rand_vale = random.choice(list_data[rand_key]) # a partir de la key generat evalue rand
    return (rand_key, rand_vale) # retorno una tuple para extract

# Función para generar datos de clickstream, usando el mismo modelo en (generate_datablognews_csv.py)
def generate_clickstream_data():
    [Article_Category, Article_Name] = select_data_rand(datos_articles_categories)
    return {
        "User_ID": random.randint(1, 1000),
        "Session_ID": random.randint(1, 100),
        "Timestamp": int(time.time()),
        "Page_URL": "https://crossdevblog.com/article/" + str(random.randint(1, 10)),
        "Time_Spent_seconds": random.randint(1, 300),
        "Clicks": random.randint(1, 50),
        "Browser_Type": random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
        "Device_Type": random.choice(['Desktop', 'Mobile', 'Tablet']),
        "Article_Category": Article_Category,
        "Article_Name": Article_Name,
        "Interacted_Elements": random.choice(['buttons', 'links', 'videos', 'images', 'forms', 'multimedia']),
        "Video_Views": random.randint(0, 5),
        "Geo_Location": random.choice([
            'USA, New York', 
            'UK, London',
            'Canada, Toronto',
            'Australia, Sydney',
            'Germany, Berlin',
            'Colombia, Bogota'
        ])
    }

producer = KafkaProducer(bootstrap_servers=[url_port], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True: # creamos un loop infinito para enviar los eventos (clickstream_data)
    clickstream_data  = generate_clickstream_data() # procesamos lo datos para cada lectura de datos en real time
    producer.send(send_event, value = clickstream_data) # los enviamos y los escuchamos en el streaming_consumer
     # vemos el dato de cada uno que genero con el mismo modelo que estamos usando, para simular los datos.
    print(f"Sent: {clickstream_data}")
    time.sleep(1) # un segundo de expera