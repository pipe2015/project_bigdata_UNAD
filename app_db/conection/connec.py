from mongoengine import connect

def conectar():
    """
    Conecta a la base de datos 'clickstream_db' en MongoDB.
    """
    connect('clickstream_db', host='localhost', port = 27017)
    print("Conexión exitosa a la base de datos clickstream_db")
    
    