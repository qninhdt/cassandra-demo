from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement
from db.base_service import BaseService
from uuid import uuid1


class CassandraService(BaseService):

    def connect(self):
        self.cluster = Cluster(["cassandra"])
        self.session = self.cluster.connect()
        self.session.execute("USE cassandra_demo;")

        self.reset()

        self.GET_USER_BY_ID_QUERY = self.session.prepare(
            """
                SELECT id, username, email
                FROM user_by_id
                WHERE id = ?;
            """
        )

        self.GET_POST_BY_USER_ID_QUERY = self.session.prepare(
            """
                SELECT id, title, content
                FROM post_by_user_id
                WHERE user_id = ?;
            """
        )

        self.CREATE_USER_QUERY = self.session.prepare(
            """
                INSERT INTO user_by_id (id, username, email, password)
                VALUES (?, ?, ?, ?);
            """
        )

        self.CREATE_POST_QUERY = self.session.prepare(
            """
                INSERT INTO post_by_user_id (user_id, id, title, content)
                VALUES (?, ?, ?, ?);
            """
        )

        print("Connected to Cassandra database")

    def disconnect(self):
        print("Disconnected from Cassandra database")
        self.cluster.shutdown()

    def get_post_by_user_id(self, user_id):
        # Fetch user data once

        user = self.session.execute(self.GET_USER_BY_ID_QUERY, (user_id,)).one()

        # Fetch all posts for the user in one query
        posts = self.session.execute(
            self.GET_POST_BY_USER_ID_QUERY,
            (user_id,),
        ).all()

        return [
            {
                "id": post.id,
                "title": post.title,
                "content": post.content,
                "user": {
                    "id": user.id,
                    "username": user.username,
                    "email": user.email,
                },
                "comments": self.get_comments_by_post_id(post.id),
            }
            for post in posts
        ]

    def get_comments_by_user_id(self, user_id):
        comments = self.session.execute(
            "SELECT id, post_id, content FROM comment_by_user_id WHERE user_id = %s;",
            (user_id,),
        ).all()

        return [
            {
                "id": comment.id,
                "post_id": comment.post_id,
                "content": comment.content,
            }
            for comment in comments
        ]

    def get_comments_by_post_id(self, post_id):
        comments = self.session.execute(
            "SELECT id, user_id, content FROM comment_by_post_id WHERE post_id = %s;",
            (post_id,),
        ).all()

        return [
            {
                "id": comment.id,
                "user": self.session.execute(
                    "SELECT id, username, email FROM user_by_id WHERE id = %s;",
                    (comment.user_id,),
                )
                .one()
                ._asdict(),
                "content": comment.content,
            }
            for comment in comments
        ]

    def get_all_user_ids(self):
        query = "SELECT id FROM user_by_id;"
        rows = self.session.execute(query)

        return [row.id for row in rows]

    def get_all_post_ids(self):
        query = "SELECT id FROM post_by_user_id;"
        rows = self.session.execute(query)

        return [row.id for row in rows]

    def create_user(self, name, email, password):
        self.session.execute(self.CREATE_USER_QUERY, (uuid1(), name, email, password))

    def create_post(self, user, title, content):
        post_id = uuid1()
        self.session.execute(self.CREATE_POST_QUERY, (user, post_id, title, content))

    def create_random_user(self, n):
        from faker import Faker

        fake = Faker()

        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i : i + n]

        for chunk in chunks(range(n), 200):
            data = [
                [uuid1(), fake.name(), fake.email(), fake.password()] for _ in chunk
            ]

            # flatten data
            data = [item for sublist in data for item in sublist]

            query = """
                BEGIN BATCH
                    {}
                APPLY BATCH;
            """.format(
                "".join(
                    [
                        "INSERT INTO user_by_id (id, username, email, password) VALUES (%s, %s, %s, %s);\n"
                        for _ in chunk
                    ]
                )
            )

            self.session.execute(query, data)

    def create_comment(self, user, post, content):
        query = """
            BEGIN BATCH
                INSERT INTO comment_by_user_id (user_id, id, post_id, content)
                VALUES (%s, %s, %s, %s);

                INSERT INTO comment_by_post_id (post_id, id, user_id, content)
                VALUES (%s, %s, %s, %s);
            APPLY BATCH;
        """

        comment_id = uuid1()
        self.session.execute(
            query,
            (
                user,
                comment_id,
                post,
                content,
                post,
                comment_id,
                user,
                content,
            ),
        )

    def reset(self):
        user_by_id_query = "DROP TABLE IF EXISTS user_by_id;"
        post_by_id_query = "DROP TABLE IF EXISTS post_by_id;"
        comment_by_id_query = "DROP TABLE IF EXISTS comment_by_id;"
        post_by_user_id_query = "DROP TABLE IF EXISTS post_by_user_id;"
        comment_by_user_id_query = "DROP TABLE IF EXISTS comment_by_user_id;"
        comment_by_post_id_query = "DROP TABLE IF EXISTS comment_by_post_id;"
        self.session.execute(user_by_id_query)
        self.session.execute(post_by_id_query)
        self.session.execute(comment_by_id_query)
        self.session.execute(post_by_user_id_query)
        self.session.execute(comment_by_user_id_query)
        self.session.execute(comment_by_post_id_query)

        # create table
        user_by_id_query = """
            CREATE TABLE IF NOT EXISTS user_by_id (
                id UUID PRIMARY KEY,
                username TEXT,
                email TEXT,
                password TEXT
            )
        """

        post_by_id_query = """
            CREATE TABLE IF NOT EXISTS post_by_id (
                id UUID PRIMARY KEY,
                user_id UUID,
                title TEXT,
                content TEXT
            );
        """

        comment_by_id_query = """
            CREATE TABLE IF NOT EXISTS comment_by_id (
                id UUID PRIMARY KEY,
                user_id UUID,
                post_id UUID,
                content TEXT
            );
        """

        post_by_user_id_query = """
            CREATE TABLE IF NOT EXISTS post_by_user_id (
                user_id UUID,
                id UUID,
                title TEXT,
                content TEXT,
                PRIMARY KEY (user_id, id)
            );
        """

        comment_by_user_id_query = """
            CREATE TABLE IF NOT EXISTS comment_by_user_id (
                user_id UUID,
                id UUID,
                post_id UUID,
                content TEXT,
                PRIMARY KEY (user_id, id)
            );
        """

        comment_by_post_id_query = """
            CREATE TABLE IF NOT EXISTS comment_by_post_id (
                post_id UUID,
                id UUID,
                user_id UUID,
                content TEXT,
                PRIMARY KEY (post_id, id)
            );
        """

        self.session.execute(user_by_id_query)
        self.session.execute(post_by_id_query)
        self.session.execute(comment_by_id_query)
        self.session.execute(post_by_user_id_query)
        self.session.execute(comment_by_user_id_query)
        self.session.execute(comment_by_post_id_query)
