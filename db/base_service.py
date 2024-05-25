from faker import Faker


class BaseService:

    def __init__(self):
        self.fake = Faker()

    def connect(self):
        pass

    def disconnect(self):
        pass

    def create_random_user(self, n):
        for _ in range(n):
            yield self.create_user(
                self.fake.name(), self.fake.email(), self.fake.password()
            )

    def create_random_post(self, n):
        user_ids = self.get_all_user_ids()

        def generator():
            for _ in range(n):
                yield self.create_post(
                    self.fake.random_element(user_ids),
                    self.fake.sentence(),
                    self.fake.text(),
                )

        return generator()

    def create_random_comment(self, n):
        user_ids = self.get_all_user_ids()
        post_ids = self.get_all_post_ids()

        def generator():
            for _ in range(n):
                yield self.create_comment(
                    self.fake.random_element(user_ids),
                    self.fake.random_element(post_ids),
                    self.fake.text(),
                )

        return generator()

    def get_all_users(self):
        pass

    def get_all_posts(self):
        pass

    def get_all_user_ids(self):
        pass

    def get_all_post_ids(self):
        pass

    def create_user(name, email, password):
        pass

    def create_post(user, title, content):
        pass

    def create_comment(user, post, content):
        pass

    def reset(self):
        pass
