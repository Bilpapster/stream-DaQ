from abc import ABC, abstractmethod


class StorageConnector(ABC):
    @abstractmethod
    def write(self, key, value):
        ...

    @abstractmethod
    def read(self, key):
        ...
