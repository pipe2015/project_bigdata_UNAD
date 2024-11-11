from mongoengine import Document, StringField, ReferenceField
from models.categories import Category

"""
- documentacion mongoengine para crear los modelos, [https://docs.mongoengine.org/guide/defining-documents.html]
- definimos le modelo para la tabla de los articulos
- creamos la referencia clave a la tabla (Category). [Donde (1) Categoria <-> puede tener <-> (*) Articulos]
"""
class Article(Document):
    # create field types table: [https://docs.mongoengine.org/guide/defining-documents.html#fields]
    article_name = StringField(required = True)
    category = ReferenceField(Category, required = True)

    def __str__(self):
        return f"Article: {self.article_name}, Category: {self.category.category_name}"