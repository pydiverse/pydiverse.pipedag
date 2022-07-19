import os
import threading
from abc import ABC, abstractmethod

import prefect
import filelock as fl

from pdpipedag.core.schema import Schema


class BaseLockManager(ABC):

    @abstractmethod
    def acquire_schema(self, schema: Schema):
        ...

    @abstractmethod
    def release_schema(self, schema: Schema):
        ...

    def is_schema_locked(self, schema: Schema) -> bool:
        ...


class FileLockManager(BaseLockManager):

    def __init__(self, base_path: str):
        self.base_path = os.path.abspath(base_path)
        self.locks: dict[str, fl.BaseFileLock] = {}
        self.logger = prefect.utilities.logging.get_logger('LockManager')

        os.makedirs(self.base_path, exist_ok = True)

    def acquire_schema(self, schema: Schema):
        lock_path = os.path.join(self.base_path, schema.name + '.lock')

        if schema.name not in self.locks:
            self.locks[schema.name] = fl.FileLock(lock_path)

        lock = self.locks[schema.name]
        if not lock.is_locked:
            self.logger.info(f"Locking schema '{schema.name}'")
        lock.acquire()

    def release_schema(self, schema: Schema):
        if schema.name not in self.locks:
            raise Exception(f"No lock for schema '{schema.name}' found.")

        lock = self.locks[schema.name]
        lock.release()

        if not lock.is_locked:
            self.logger.info(f"Unlocking schema '{schema.name}'")
            os.remove(lock.lock_file)
            del self.locks[schema.name]

    def is_schema_locked(self, schema: Schema) -> bool:
        if schema.name not in self.locks:
            return False
        return self.locks[schema.name].is_locked
