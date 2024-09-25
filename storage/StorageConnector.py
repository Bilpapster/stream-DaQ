from abc import ABC, abstractmethod


class StorageConnector(ABC):
    @abstractmethod
    def write(self, key, value):
        pass

    @abstractmethod
    def read(self, key):
        pass