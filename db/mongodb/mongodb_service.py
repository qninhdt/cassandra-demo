from db.base_service import BaseService
from mongoengine import *
from datetime import datetime


class User(Document):
    username = StringField(required=True, max_length=255)
    email = EmailField(required=True, max_length=255)
    password = StringField(required=True, max_length=255)
    created_at = DateTimeField(default=datetime.now)
    updated_at = DateTimeField(default=datetime.now)

    meta = {"collection": "users"}


class Post(Document):
    user = ReferenceField(User, reverse_delete_rule=CASCADE)
    title = StringField(required=True, max_length=255)
    content = StringField(required=True)
    created_at = DateTimeField(default=datetime.now)
    updated_at = DateTimeField(default=datetime.now)

    meta = {"collection": "posts"}


class Comment(Document):
    user = ReferenceField(User, reverse_delete_rule=CASCADE)
    post = ReferenceField(Post, reverse_delete_rule=CASCADE)
    content = StringField(required=True)
    created_at = DateTimeField(default=datetime.now)
    updated_at = DateTimeField(default=datetime.now)

    meta = {"collection": "comments"}


class MongoDBService(BaseService):

    def connect(self):
        try:
            connect("cassandra_demo", host="mongodb", port=27017)
            print("Connected to MongoDB database")
        except Exception as e:
            print("Cannot connect to MongoDB database", e)

    def disconnect(self):
        disconnect()
        print("Disconnected from MongoDB database")

    def get_all_users(self):
        for user in User.objects.all():
            yield [
                {
                    "id": str(user.id),
                    "username": user.username,
                    "email": user.email,
                }
            ]

    def get_all_posts(self):
        for post in Post.objects.all():
            yield [
                {
                    "id": str(post.id),
                    "title": post.title,
                    "content": post.content,
                    "user": {
                        "id": str(post.user.id),
                        "username": post.user.username,
                        "email": post.user.email,
                    },
                    "comments": [
                        {
                            "id": str(comment.id),
                            "content": comment.content,
                            "user": {
                                "id": str(comment.user.id),
                                "username": comment.user.username,
                                "email": comment.user.email,
                            },
                        }
                        for comment in Comment.objects(post=post)
                    ],
                }
            ]

    def get_all_user_ids(self):
        return [str(user.id) for user in User.objects.all()]

    def get_all_post_ids(self):
        return [str(post.id) for post in Post.objects.all()]

    def create_user(self, name, email, password):
        User(username=name, email=email, password=password).save()

    def create_post(self, user, title, content):
        Post(user=user, title=title, content=content).save()

    def create_comment(self, user, post, content):
        Comment(user=user, post=post, content=content).save()

    def reset(self):
        Comment.objects.delete()
        Post.objects.delete()
        User.objects.delete()

        print("Reset MongoDB database")
