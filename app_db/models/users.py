from mongoengine import Document, StringField, ReferenceField
from datetime import datetime

# documentacion mongoengine para crear los modelos, [https://docs.mongoengine.org/guide/defining-documents.html]
#definimos le modelo para la tabla de la usars => Guarda los registros de caad usuario 
class User(Document):
    name = StringField(required = True)
    geo_location = StringField()

    def __str__(self): # funcion para retormas los campos cundo de llame la clase
        return f"User: {self.name}, Location: {self.geo_location}"