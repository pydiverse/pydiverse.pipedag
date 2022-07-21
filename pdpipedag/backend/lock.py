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
    """Lock State

    Represent the current state of a lock.

    UNLOCKED:
        The lock manager hasn't acquired this lock and a different process
        might be accessing the resource.

    LOCKED:
        The lock manager has acquired the lock, and it hasn't expired.

    UNCERTAIN:
        The lock manager isn't certain about the state of the lock. This
        can, for example, happen if a lock manager requires a connection to
        the internet and this connection is interrupted.
        Any process that depends on the locked resource should assume that
        the resource isn't locked anymore and pause until the lock transitions
        to a different state.

    INVALID:
        The lock has been invalidated. This means that a lock that was LOCKED
        has been unlocked for some unexpected reason. A common transition
        is that a lock transitions from `LOCKED -> UNCERTAIN -> INVALID`.
    """

    UNLOCKED = 'UNLOCKED'
    LOCKED = 'LOCKED'
    UNCERTAIN = 'UNCERTAIN'
    INVALID = 'INVALID'


LockStateListener: TypeAlias = Callable[[Schema, LockState, LockState], None]


class BaseLockManager(ABC):
    """Lock Manager base class

    A lock manager is responsible for acquiring and releasing locks on
    schemas. This is necessary to prevent two flows from accessing the
    same schema at the same time (which would lead to corrupted data).
    """

    def __init__(self):
        self.logger = prefect.utilities.logging.get_logger(type(self).__name__)

        self.state_listeners = set()
        self.lock_states = defaultdict(lambda: LockState.UNLOCKED)
        self.__lock_state_lock = threading.Lock()

    @abstractmethod
    def acquire_schema(self, schema: Schema):
        """Acquires a lock to access the given schema"""

    @abstractmethod
    def release_schema(self, schema: Schema):
        """Releases a previously acquired lock on a schema"""

    def add_lock_state_listener(self, listener: LockStateListener):
        """Add a function to be called when the state of a lock changes

        The listener will be called with the affected schema, the old lock
        state and the new lock state as arguments.
        """
        if listener is None or not callable(listener):
            raise ValueError('Listener must be callable.')
        self.state_listeners.add(listener)

    def remove_lock_state_listener(self, listener: LockStateListener):
        """Removes a function from the set of listeners"""
        self.state_listeners.remove(listener)

    def set_lock_state(self, schema: Schema, new_state: LockState):
        """Update the state of a lock

        Function used by lock implementations to update the state of a
        schema lock. If appropriate, listeners will be informed about
        this change.
        """
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
        """Returns the state of a schema lock"""
        with self.__lock_state_lock:
            return self.lock_states[schema]


class NoLockManager(BaseLockManager):
    """Non locking lock manager (oxymoron)

    This lock manager doesn't do any locking and only serves as a placeholder
    for an actual lock manager for testing something locally.

    .. WARNING::
        DON'T USE THIS IN A PRODUCTION ENVIRONMENT. A LOCK MANAGER IS
        ESSENTIAL TO PREVENT DATA CORRUPTION.
    """

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
    """Lock manager that uses lock files

    For details on how exactly the file locking is implemented, check out the
    `filelock documentation`_.

    .. _`filelock documentation`:
        https://py-filelock.readthedocs.io/en/latest/index.html
    """

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
    """Apache ZooKeeper based lock manager

    Uses Apache ZooKeeper to establish `fully distributed locks that are
    globally synchronous` [1]_. The advantage of this approach is that we
    it is highly reliable and that in case our flow crashes, the acquired
    locks automatically get released (the locks are ephemeral).

    .. [1] https://zookeeper.apache.org/doc/r3.1.2/recipes.html#sc_recipes_Locks
    """

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
