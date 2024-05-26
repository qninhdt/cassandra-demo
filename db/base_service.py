from abc import abstractmethod
from faker import Faker
from random import shuffle
from multiprocessing.pool import ThreadPool


class BaseService:

    def __init__(self):
        self.fake = Faker()

    def connect(self):
        pass

    def disconnect(self):
        pass

    def create_random_user(self, n):
        for _ in range(n):
            self.create_user(
                self.fake.name(),
                self.fake.email(),
                self.fake.password(),
            )

    def create_random_post(self, n):
        user_ids = self.get_all_user_ids()
        pool = ThreadPool(processes=1)

        def run(_):
            self.create_post(
                self.fake.random_element(user_ids),
                self.fake.sentence(),
                self.fake.text(),
            )

        pool.map(run, range(n), 1)

    def create_random_comment(self, n):
        user_ids = self.get_all_user_ids()
        post_ids = self.get_all_post_ids()
        pool = ThreadPool(processes=1)

        def run(_):
            self.create_comment(
                self.fake.random_element(user_ids),
                self.fake.random_element(post_ids),
                self.fake.text(),
            )

        pool.map(run, range(n), 1)

    def get_posts_by_users(self):
        user_ids = list(self.get_all_user_ids())
        shuffle(user_ids)

        def generator():
            for user_id in user_ids:
                yield self.get_post_by_user_id(user_id)

        return generator()

    def get_comments_by_users(self):
        user_ids = list(self.get_all_user_ids())
        shuffle(user_ids)

        def generator():
            for user_id in user_ids:
                yield self.get_comments_by_user_id(user_id)

        return generator()

    def get_comments_by_posts(self):
        post_ids = list(self.get_all_post_ids())
        shuffle(post_ids)

        def generator():
            for post_id in post_ids:
                yield self.get_comments_by_post_id(post_id)

        return generator()

    @abstractmethod
    def get_post_by_user_id(self, user_id):
        pass

    @abstractmethod
    def get_comments_by_user_id(self, user_id):
        pass

    @abstractmethod
    def get_comments_by_post_id(self, post_id):
        pass

    @abstractmethod
    def get_all_user_ids(self):
        pass

    @abstractmethod
    def get_all_post_ids(self):
        pass

    @abstractmethod
    def create_user(self, name, email, password):
        pass

    @abstractmethod
    def create_post(self, user, title, content):
        pass

    @abstractmethod
    def create_comment(self, user, post, content):
        pass

    @abstractmethod
    def reset(self):
        pass
