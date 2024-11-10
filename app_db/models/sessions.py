from mongoengine import Document, StringField, ReferenceField, DateTimeField
from datetime import datetime
from models.users import User # importamos la clase del modelo users

class Session(Document):
    session_id = StringField(primary_key = True)
    user = ReferenceField(User, required = True)
    timestamp = DateTimeField(default = datetime.now)
    browser_type = StringField(choices = ["Chrome", "Firefox", "Safari", "Edge"])
    device_type = StringField(choices = ["Desktop", "Mobile", "Tablet"])

    def __str__(self):
        return f"Session {self.session_id} by {self.user.name}"