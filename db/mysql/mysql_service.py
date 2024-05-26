import os
import mysql.connector
from db.base_service import BaseService


class MySQLService(BaseService):

    def connect(self):
        self.db = mysql.connector.connect(
            database="cassandra_demo",
            user="root",
            password=os.getenv("MYSQL_ROOT_PASSWORD"),
            host="mysql",
            port=3306,
        )

        self.cursor = self.db.cursor()

        self.reset()

        print("Connected to MySQL database")

    def disconnect(self):
        self.db.close()
        print("Disconnected from MySQL database")

    def get_post_by_user_id(self, user_id):
        # join author information
        query = """
            SELECT posts.id, posts.title, posts.content, users.id, users.username, users.email
            FROM posts
            JOIN users ON posts.user_id = users.id
            WHERE users.id = %s
        """

        self.cursor.execute(query, (user_id,))
        posts_ = self.cursor.fetchall()

        posts = []

        for post_ in posts_:
            post = {
                "id": post_[0],
                "title": post_[1],
                "content": post_[2],
                "user": {
                    "id": post_[3],
                    "username": post_[4],
                    "email": post_[5],
                },
                "comments": [],
            }

            # get comments
            query = """
                SELECT comments.id, comments.content, users.id, users.username, users.email
                FROM comments
                JOIN users ON comments.user_id = users.id
                WHERE comments.post_id = %s
            """

            self.cursor.execute(query, (post["id"],))
            comments_ = self.cursor.fetchall()

            for comment_ in comments_:
                comment = {
                    "id": comment_[0],
                    "content": comment_[1],
                    "user": {
                        "id": comment_[2],
                        "username": comment_[3],
                        "email": comment_[4],
                    },
                }

                post["comments"].append(comment)

            posts.append(post)

        return posts

    def get_comments_by_user_id(self, user_id):
        query = """
            SELECT comments.id, comments.content, posts.id, posts.title, posts.content
            FROM comments
            JOIN posts ON comments.post_id = posts.id
            WHERE comments.user_id = %s
        """

        self.cursor.execute(query, (user_id,))
        comments_ = self.cursor.fetchall()

        comments = []

        for comment_ in comments_:
            comment = {
                "id": comment_[0],
                "content": comment_[1],
                "post": {
                    "id": comment_[2],
                    "title": comment_[3],
                    "content": comment_[4],
                },
            }

            comments.append(comment)

        return comments

    def get_comments_by_post_id(self, post_id):
        query = """
            SELECT comments.id, comments.content, users.id, users.username, users.email
            FROM comments
            JOIN users ON comments.user_id = users.id
            WHERE comments.post_id = %s
        """

        self.cursor.execute(query, (post_id,))
        comments_ = self.cursor.fetchall()

        comments = []

        for comment_ in comments_:
            comment = {
                "id": comment_[0],
                "content": comment_[1],
                "user": {
                    "id": comment_[2],
                    "username": comment_[3],
                    "email": comment_[4],
                },
            }

            comments.append(comment)

        return comments

    def get_all_user_ids(self):
        self.cursor.execute("SELECT id FROM users")
        rs = [r[0] for r in self.cursor.fetchall()]
        return rs

    def get_all_post_ids(self):
        self.cursor.execute("SELECT id FROM posts")
        rs = [r[0] for r in self.cursor.fetchall()]
        return rs

    def create_user(self, name, email, password):
        query = """
            INSERT INTO users (username, email, password)
            VALUES (%s, %s, %s)
        """

        self.cursor.execute(query, (name, email, password))
        self.db.commit()

    def create_post(self, user, title, content):
        query = """
            INSERT INTO posts (user_id, title, content)
            VALUES (%s, %s, %s)
        """

        self.cursor.execute(query, (user, title, content))
        self.db.commit()

    def create_comment(self, user, post, content):
        query = """
            INSERT INTO comments (user_id, post_id, content)
            VALUES (%s, %s, %s)
        """

        self.cursor.execute(query, (user, post, content))
        self.db.commit()

    def reset(self):
        for table in ["comments", "posts", "users"]:
            query = f"DELETE FROM {table}"

            self.cursor.execute(query)
            self.db.commit()

        # create table
        user_query = """
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                password VARCHAR(255) NOT NULL
            );
        """

        post_query = """
            CREATE TABLE IF NOT EXISTS posts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
        """

        comment_query = """
            CREATE TABLE IF NOT EXISTS comments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                post_id INT,
                content TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (post_id) REFERENCES posts(id)
            );
        """

        self.cursor.execute(user_query)
        self.db.commit()

        self.cursor.execute(post_query)
        self.db.commit()

        self.cursor.execute(comment_query)
        self.db.commit()
