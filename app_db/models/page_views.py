from mongoengine import Document, StringField, IntField,ReferenceField, ListField, EmbeddedDocument, EmbeddedDocumentField
from models.sessions import Session
from models.categories import Category
from models.articles import Article

"""
- documentacion mongoengine para crear los modelos, [https://docs.mongoengine.org/guide/defining-documents.html]
- definimos le modelo para la tabla de pageView = esta tabla es para guardar la analitica de cada pagina
- creamos la referencias clave a la tabla (Category, Article). 
R1: [Donde (1) PageView <-> puede tener <-> (1) Category]
R2: [Donde (1) PageView <-> puede tener <-> (1) Article]
"""
class PageView(Document):
    page_url = StringField(required=True)
    session = ReferenceField(Session, required=True)
    time_spent_seconds = IntField()
    clicks = IntField()
    category = ReferenceField(Category, required=True)
    article = ReferenceField(Article, required=True)
    interacted_elements = ListField(StringField())
    video_views = IntField()

    def __str__(self):
        return f"Page URL: {self.page_url}, Category: {self.category.category_name}, Article: {self.article.article_name}"
