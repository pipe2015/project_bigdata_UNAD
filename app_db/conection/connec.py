# miportamos las libreria 
from mongoengine import connect

# documentacion mongoengine para crear la conexion, [https://docs.mongoengine.org/guide/connecting.html#guide-connecting]
# Se definen las variables de conexion, unsando la base de datos (MongoDB)
DB_HOST = "localhost" # el host por defecto que es: [127.0.0.1]
DB_NAME = "clickstream_db" # la base de datos, creada en mongocompass
DB_USER = "crossdev" # el nombre del usuario, admin creado
DB_PASSWORD = "cross" # la contrasela del usuario, admin creado
AUTH_SOURCE = "admin" # Propiedada que establexe, que la conexion debe ser autenticada usando la table admin, donde
# se creo el usuario
DB_PORT = 27017 # default puerto de mongoDB

def connect_database():
    """
    Conecta a la base de datos MongoDB usando credenciales separadas.
    Si hay un error en la conexión, lanza una excepción.
    """
    # creamos la ruta, usando f fotmar para crear la conexion
    url = f"mongodb://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?authSource={AUTH_SOURCE}"
    try:
        connect(host = url) # pasamos la base de datos que nos queremos conextar
        print("Conexion exitosa a la base de datos", DB_NAME)
    except Exception as CE:
        print(f"Error conexion: {CE}")
        raise Exception(f"No se pudo conectar a la base de datos {DB_NAME} de MongoDB")
    
    