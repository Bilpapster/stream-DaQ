import redis
from StorageConnector import StorageConnector


class RedisConnector(StorageConnector):
    def __init__(self):
        self.connection = redis.Redis(host='localhost', port=6379, decode_responses=True)

    def write(self, key, value):
        return self.connection.set(key, value)

    def read(self, key):
        return self.connection.get(key)
