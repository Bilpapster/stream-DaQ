import redis
from StorageConnector import StorageConnector


class RedisConnector(StorageConnector):
    def __init__(self):
        self.connection = redis.Redis(host='localhost', port=6379, decode_responses=True)

    def write(self, key, value):
        try:
            self.connection.set(key, value)
            return True
        except Exception as e:
            # print(e) # we could possibly send the exception message to some warning/log channel in the future
            return False

    def read(self, key):
        return self.connection.get(key)
