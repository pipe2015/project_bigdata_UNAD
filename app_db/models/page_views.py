from mongoengine import Document, StringField, ReferenceField, ListField, EmbeddedDocument, EmbeddedDocumentField
from models.sessions import Session
from models.categories import Category
from models.articles import Article

class PageView(EmbeddedDocument):
    _id = StringField(primary_key = True)
    page_url = StringField(required=True)
    session = ReferenceField(Session, required=True)
    time_spent_seconds = StringField()
    clicks = StringField()
    category = ReferenceField(Category, required=True)
    article = ReferenceField(Article, required=True)
    interacted_elements = ListField(StringField())
    video_views = StringField()

    def __str__(self):
        return f"Page URL: {self.page_url}, Category: {self.category.category_name}, Article: {self.article.article_name}"
