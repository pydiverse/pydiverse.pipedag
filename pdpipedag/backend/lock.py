from __future__ import annotations

import os
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum
from typing import Callable, TypeAlias

import prefect

from pdpipedag.util import requires
from pdpipedag.core.schema import Schema
from pdpipedag.errors import LockError

__all__ = [
    'BaseLockManager',
    'LockState',
    'NoLockManager',
    'FileLockManager',
    'ZooKeeperLockManager',
]


class LockState(str, Enum):
    UNLOCKED = 'UNLOCKED'
    LOCKED = 'LOCKED'
    UNCERTAIN = 'UNCERTAIN'
    INVALID = 'INVALID'


LockStateListener: TypeAlias = Callable[[Schema, LockState, LockState], None]


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

    def add_lock_state_listener(self, listener: LockStateListener):
        if listener is None or not callable(listener):
            raise ValueError('Listener must be callable.')
        self.state_listeners.add(listener)

    def remove_lock_state_listener(self, listener: LockStateListener):
        self.state_listeners.remove(listener)

    def set_lock_state(self, schema: Schema, new_state: LockState):
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

    def get_lock_state(self, schema: Schema) -> LockState:
        with self.__lock_state_lock:
            return self.lock_states[schema]


class NoLockManager(BaseLockManager):

    def acquire_schema(self, schema: Schema):
        self.set_lock_state(schema, LockState.LOCKED)

    def release_schema(self, schema: Schema):
        self.set_lock_state(schema, LockState.UNLOCKED)


try:
    import filelock as fl
except ImportError:
    fl = None


@requires(fl, ImportError("FileLockManager requires 'filelock' to be installed."))
class FileLockManager(BaseLockManager):

    def __init__(self, base_path: str):
        super().__init__()
        self.base_path = os.path.abspath(base_path)
        self.locks: dict[Schema, fl.BaseFileLock] = {}

        os.makedirs(self.base_path, exist_ok = True)

    def acquire_schema(self, schema: Schema):
        lock_path = os.path.join(self.base_path, schema.name + '.lock')

        if schema not in self.locks:
            self.locks[schema] = fl.FileLock(lock_path)

        lock = self.locks[schema]
        if not lock.is_locked:
            self.logger.info(f"Locking schema '{schema.name}'")
        lock.acquire()
        self.set_lock_state(schema, LockState.LOCKED)

    def release_schema(self, schema: Schema):
        if schema not in self.locks:
            raise LockError(f"No lock for schema '{schema.name}' found.")

        lock = self.locks[schema]
        lock.release()

        if not lock.is_locked:
            self.logger.info(f"Unlocking schema '{schema.name}'")
            os.remove(lock.lock_file)
            del self.locks[schema]
            self.set_lock_state(schema, LockState.UNLOCKED)


try:
    import kazoo
    from kazoo.client import KazooClient, KazooState
    from kazoo.recipe.lock import Lock as KazooLock
except ImportError:
    kazoo = None


@requires(kazoo, ImportError("ZooKeeperLockManager requires 'kazoo' to be installed."))
class ZooKeeperLockManager(BaseLockManager):

    def __init__(self, client: KazooClient):
        super().__init__()

        self.client = client
        if not self.client.connected:
            self.client.start()
        self.client.add_listener(self._lock_listener)

        self.locks: dict[Schema, KazooLock] = {}

    def acquire_schema(self, schema: Schema):
        lock = self.client.Lock('/pipedag/locks/' + schema.name, )
        self.logger.info(f"Locking schema '{schema.name}'")
        if not lock.acquire():
            raise LockError(f"Failed to acquire lock for schema '{schema.name}'")
        self.locks[schema] = lock
        self.set_lock_state(schema, LockState.LOCKED)

    def release_schema(self, schema: Schema):
        if schema not in self.locks:
            raise LockError(f"No lock for schema '{schema.name}' found.")

        self.logger.info(f"Unlocking schema '{schema.name}'")
        self.locks[schema].release()
        del self.locks[schema]
        self.set_lock_state(schema, LockState.UNLOCKED)

    def _lock_listener(self, state):
        if state == KazooState.SUSPENDED:
            for schema in self.locks.keys():
                self.set_lock_state(schema, LockState.UNCERTAIN)
        elif state == KazooState.LOST:
            for schema in self.locks.keys():
                self.set_lock_state(schema, LockState.INVALID)
        elif state == KazooState.CONNECTED:
            for schema in self.locks.keys():
                self.set_lock_state(schema, LockState.LOCKED)
