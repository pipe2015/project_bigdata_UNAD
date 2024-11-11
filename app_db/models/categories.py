from mongoengine import Document, StringField
from datetime import datetime

# documentacion mongoengine para crear los modelos, [https://docs.mongoengine.org/guide/defining-documents.html]
#definimos le modelo para la tabla de los categorias
class Category(Document):
    category_name = StringField(required = True)

    def __str__(self):
        return f"Category: {self.category_name}"