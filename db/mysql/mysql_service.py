import os
from datetime import datetime
from peewee import *
from playhouse.mysql_ext import MySQLConnectorDatabase
from db.base_service import BaseService
from playhouse.shortcuts import model_to_dict

db = MySQLConnectorDatabase(
    database="cassandra_demo",
    user="root",
    password=os.getenv("MYSQL_ROOT_PASSWORD"),
    host="mysql",
    port=3306,
    autoconnect=False,
)


class BaseModel(Model):
    class Meta:
        database = db


class User(BaseModel):
    username = CharField(max_length=255)
    email = CharField(max_length=255)
    password = CharField(max_length=255)

    class Meta:
        table_name = "users"


class Post(BaseModel):
    user = ForeignKeyField(User, backref="posts")
    title = CharField(max_length=255)
    content = TextField()

    class Meta:
        table_name = "posts"


class Comment(BaseModel):
    user = ForeignKeyField(User, backref="comments")
    post = ForeignKeyField(Post, backref="comments")
    content = TextField()

    class Meta:
        table_name = "comments"


class MySQLService(BaseService):

    def connect(self):
        try:
            db.connect()
            db.create_tables([User, Post, Comment])

            print("Connected to MySQL database")
        except Exception as e:
            print("Cannot connect to MySQL database", e)

    def disconnect(self):
        db.close()
        print("Disconnected from MySQL database")

    def get_all_users(self):
        for user in User.select():
            yield [
                {
                    "id": user.id,
                    "username": user.username,
                    "email": user.email,
                }
            ]

    def get_all_posts(self):
        for post in Post.select():
            yield [
                {
                    "id": post.id,
                    "title": post.title,
                    "content": post.content,
                    "user": {
                        "id": post.user.id,
                        "username": post.user.username,
                        "email": post.user.email,
                    },
                    "comments": [
                        {
                            "id": comment.id,
                            "user": {
                                "id": comment.user.id,
                                "username": comment.user.username,
                                "email": comment.user.email,
                            },
                            "content": comment.content,
                        }
                        for comment in post.comments
                    ],
                }
            ]

    def get_all_user_ids(self):
        return [r[0] for r in db.execute_sql("SELECT id FROM users")]

    def get_all_post_ids(self):
        return [r[0] for r in db.execute_sql("SELECT id FROM posts")]

    def create_user(self, name, email, password):
        User.create(username=name, email=email, password=password)

    def create_post(self, user, title, content):
        Post.create(user=user, title=title, content=content)

    def create_comment(self, user, post, content):
        Comment.create(user=user, post=post, content=content)

    def reset(self):
        Comment.delete().execute()
        Post.delete().execute()
        User.delete().execute()

        db.execute_sql("ALTER TABLE users AUTO_INCREMENT = 1")
        db.execute_sql("ALTER TABLE posts AUTO_INCREMENT = 1")
        db.execute_sql("ALTER TABLE comments AUTO_INCREMENT = 1")

        print("Reset MySQL database")
