from mongoengine import Document, StringField
from datetime import datetime

class Category(Document):
    _id = StringField(primary_key = True)
    category_name = StringField(required = True)

    def __str__(self):
        return f"Category: {self.category_name}"