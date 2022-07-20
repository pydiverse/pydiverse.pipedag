import os
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum
from typing import Callable

import prefect
import filelock as fl

from pdpipedag.core.schema import Schema
from pdpipedag.errors import LockError


class BaseLockManager(ABC):

    def __init__(self):
        self.logger = prefect.utilities.logging.get_logger(type(self).__name__)

        self.state_listeners = set()
        self.lock_states = defaultdict(lambda: LockState.UNLOCKED)
        self.__lock_state_lock = threading.Lock()

    @abstractmethod
    def acquire_schema(self, schema: Schema):
        ...

    @abstractmethod
    def release_schema(self, schema: Schema):
        ...

    def add_lock_state_listener(self, listener: Callable[[str, 'LockState', 'LockState'], None]):
        if listener is None or not callable(listener):
            raise ValueError('Listener must be callable.')
        self.state_listeners.add(listener)

    def remove_lock_state_listener(self, listener: Callable[[str, 'LockState'], None]):
        self.state_listeners.remove(listener)

    def _set_lock_state(self, schema: str, new_state: 'LockState'):
        with self.__lock_state_lock:
            if schema not in self.lock_states:
                self.lock_states[schema] = new_state
                for listener in self.state_listeners:
                    listener(schema, LockState.UNLOCKED, new_state)
            else:
                old_state = self.lock_states[schema]
                self.lock_states[schema] = new_state
                if old_state != new_state:
                    for listener in self.state_listeners:
                        listener(schema, old_state, new_state)

            if new_state == LockState.UNLOCKED:
                del self.lock_states[schema]

    def get_lock_state(self, schema: str) -> 'LockState':
        with self.__lock_state_lock:
            return self.lock_states[schema]


class LockState(str, Enum):
    UNLOCKED = 'UNLOCKED'
    LOCKED = 'LOCKED'
    UNCERTAIN = 'UNCERTAIN'
    INVALID = 'INVALID'


class FileLockManager(BaseLockManager):

    def __init__(self, base_path: str):
        super().__init__()
        self.base_path = os.path.abspath(base_path)
        self.locks: dict[str, fl.BaseFileLock] = {}

        os.makedirs(self.base_path, exist_ok = True)

    def acquire_schema(self, schema: Schema):
        lock_path = os.path.join(self.base_path, schema.name + '.lock')

        if schema.name not in self.locks:
            self.locks[schema.name] = fl.FileLock(lock_path)

        lock = self.locks[schema.name]
        if not lock.is_locked:
            self.logger.info(f"Locking schema '{schema.name}'")
        lock.acquire()
        self._set_lock_state(schema.name, LockState.LOCKED)

    def release_schema(self, schema: Schema):
        if schema.name not in self.locks:
            raise Exception(f"No lock for schema '{schema.name}' found.")

        lock = self.locks[schema.name]
        lock.release()

        if not lock.is_locked:
            self.logger.info(f"Unlocking schema '{schema.name}'")
            os.remove(lock.lock_file)
            del self.locks[schema.name]
            self._set_lock_state(schema.name, LockState.UNLOCKED)

    def is_schema_locked(self, schema: Schema) -> bool:
        if schema.name not in self.locks:
            return False
        return self.locks[schema.name].is_locked
