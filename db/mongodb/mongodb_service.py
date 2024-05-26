from db.base_service import BaseService
from datetime import datetime
import pymongo


class MongoDBService(BaseService):

    def connect(self):
        self.client = pymongo.MongoClient("mongodb://mongodb:27017")
        self.db = self.client["cassandra_demo"]

        self.users = self.db["users"]
        self.posts = self.db["posts"]
        self.comments = self.db["comments"]

        self.reset()

        print("Connected to MongoDB database")

    def disconnect(self):
        print("Disconnected from MongoDB database")
        self.db.close()

    def get_post_by_user_id(self, user_id):
        # Find user details
        user = self.users.find_one({"_id": user_id})

        # Find posts and their comments with user details
        posts = list(self.posts.find({"user": user_id}))

        for post in posts:
            post["user"] = user
            post["comments"] = self.get_comments_by_post_id(post["_id"])

        return posts

    def get_comments_by_user_id(self, user_id):
        comments = self.comments.find({"user": user_id})

        return list(comments)

    def get_comments_by_post_id(self, post_id):
        comments = list(self.comments.find({"post": post_id}))

        # Fetch user details for each comment
        for comment in comments:
            comment["user"] = self.users.find_one({"_id": comment["user"]})

        return comments

    def get_all_user_ids(self):
        return [user["_id"] for user in self.users.find()]

    def get_all_post_ids(self):
        return [post["_id"] for post in self.posts.find()]

    def create_user(self, name, email, password):
        user = {
            "name": name,
            "email": email,
            "password": password,
        }

        self.users.insert_one(user)

    def create_post(self, user, title, content):
        post = {
            "user": user,
            "title": title,
            "content": content,
        }

        self.posts.insert_one(post)

    def create_comment(self, user, post, content):
        comment = {
            "user": user,
            "post": post,
            "content": content,
        }

        self.comments.insert_one(comment)

    def reset(self):
        self.users.drop()
        self.posts.drop()
        self.comments.drop()

        # create index
        self.posts.create_index("user")
        self.comments.create_index("user")
        self.comments.create_index("post")
