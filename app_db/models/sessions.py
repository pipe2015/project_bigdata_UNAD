from mongoengine import Document, StringField, ReferenceField, DateTimeField
from datetime import datetime
from models.users import User # importamos la clase del modelo users

"""
- documentacion mongoengine para crear los modelos, [https://docs.mongoengine.org/guide/defining-documents.html].
- definimos le modelo para la tabla de las sessiones = Guarda la session del uusario por navegador, usando cookies.
- creamos la referencia clave a la tabla (Users). [Donde (1) Usuario <-> puede tener <-> (*) Sessiones]
"""
class Session(Document):
    session_id = StringField()
    user = ReferenceField(User, required = True) 
    timestamp = DateTimeField(default = datetime.now)
    browser_type = StringField(choices = ["Chrome", "Firefox", "Safari", "Edge"])
    device_type = StringField(choices = ["Desktop", "Mobile", "Tablet"])

    def __str__(self):
        return f"Session {self.session_id} by {self.user.name}"