from cassandra.cqlengine.connection import setup
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *
from cassandra.cqlengine.management import sync_table
from datetime import datetime
from db.base_service import BaseService


class User(Model):
    username = Text()
    email = Text()
    password = Text()
    created_at = DateTime(default=datetime.now)
    updated_at = DateTime(default=datetime.now)


class Post(Model):
    user = Text(primary_key=True)
    title = Text(primary_key=True)
    content = Text()
    created_at = DateTime(default=datetime.now)
    updated_at = DateTime(default=datetime.now)


class Comment(Model):
    user = Text(primary_key=True)
    post = Text(primary_key=True)
    content = Text()
    created_at = DateTime(default=datetime.now)
    updated_at = DateTime(default=datetime.now)


def connect_cassandra():
    try:
        setup(["cassandra"], "cassandra_demo")
        print("Connected to Cassandra database")
    except Exception as e:
        print("Cannot connect to Cassandra database", e)

    sync_table(User)
    sync_table(Post)
    sync_table(Comment)
