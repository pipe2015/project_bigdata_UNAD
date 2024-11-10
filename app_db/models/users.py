from mongoengine import Document, StringField, ReferenceField
from datetime import datetime

#creamos el modelos y lo extendemos de document y le asignaos los campos
class User(Document):
    _id = StringField(primary_key = True)
    name = StringField(required = True)
    geo_location = StringField()

    def __str__(self): # funcion para retormas los campos cundo de llame la clase
        return f"User: {self.name}, Location: {self.geo_location}"