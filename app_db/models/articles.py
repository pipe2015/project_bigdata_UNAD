from mongoengine import Document, StringField, ReferenceField
from models.categories import Category

class Article(Document):
    _id = StringField(primary_key = True)
    article_name = StringField(required = True)
    category = ReferenceField(Category, required = True)

    def __str__(self):
        return f"Article: {self.article_name}, Category: {self.category.category_name}"