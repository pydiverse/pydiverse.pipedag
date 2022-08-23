from __future__ import annotations

import atexit
import os
import threading
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from enum import Enum
from typing import Any, Callable, Union

import structlog

from pydiverse.pipedag.context.context import ConfigContext
from pydiverse.pipedag.core.stage import Stage
from pydiverse.pipedag.errors import LockError
from pydiverse.pipedag.util import normalize_name, requires

__all__ = [
    "BaseLockManager",
    "LockState",
    "NoLockManager",
    "FileLockManager",
    "ZooKeeperLockManager",
]


class LockState(Enum):
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

    UNLOCKED = 0
    LOCKED = 1
    UNCERTAIN = 2
    INVALID = 3


Lockable = Union[Stage, str]
LockStateListener = Callable[[Lockable, LockState, LockState], None]


class BaseLockManager(ABC):
    """Lock Manager base class

    A lock manager is responsible for acquiring and releasing locks on
    stage. This is necessary to prevent two flows from accessing the
    same stage at the same time (which would lead to corrupted data).
    """

    def __init__(self):
        self.logger = structlog.get_logger(lock=type(self).__name__)

        self.state_listeners = set()
        self.lock_states = defaultdict(lambda: LockState.UNLOCKED)
        self.__lock_state_lock = threading.Lock()

    @contextmanager
    def __call__(self, lock: Lockable):
        self.acquire(lock)
        try:
            yield
        finally:
            self.release(lock)

    def close(self):
        """Clean up and close all open resources"""

    @abstractmethod
    def acquire(self, lock: Lockable):
        """Acquires a lock to access a given object"""

    @abstractmethod
    def release(self, lock: Lockable):
        """Releases a previously acquired lock"""

    def add_lock_state_listener(self, listener: LockStateListener):
        """Add a function to be called when the state of a lock changes

        The listener will be called with the affected stage, the old lock
        state and the new lock state as arguments.
        """
        if listener is None or not callable(listener):
            raise ValueError("Listener must be callable.")
        self.state_listeners.add(listener)

    def remove_lock_state_listener(self, listener: LockStateListener):
        """Removes a function from the set of listeners"""
        self.state_listeners.remove(listener)

    def set_lock_state(self, lock: Lockable, new_state: LockState):
        """Update the state of a lock

        Function used by lock implementations to update the state of a
        lock. If appropriate, listeners will be informed about this change.
        """
        with self.__lock_state_lock:
            if lock not in self.lock_states:
                self.lock_states[lock] = new_state
                for listener in self.state_listeners:
                    listener(lock, LockState.UNLOCKED, new_state)
            else:
                old_state = self.lock_states[lock]
                self.lock_states[lock] = new_state
                if old_state != new_state:
                    for listener in self.state_listeners:
                        listener(lock, old_state, new_state)

            if new_state == LockState.UNLOCKED:
                del self.lock_states[lock]

    def get_lock_state(self, lock: Lockable) -> LockState:
        """Returns the state of a lock"""
        with self.__lock_state_lock:
            return self.lock_states[lock]


class NoLockManager(BaseLockManager):
    """Non locking lock manager (oxymoron)

    This lock manager doesn't do any locking and only serves as a placeholder
    for an actual lock manager for testing something locally.

    .. WARNING::
        DON'T USE THIS IN A PRODUCTION ENVIRONMENT. A LOCK MANAGER IS
        ESSENTIAL TO PREVENT DATA CORRUPTION.
    """

    def acquire(self, lock: Lockable):
        self.set_lock_state(lock, LockState.LOCKED)

    def release(self, lock: Lockable):
        self.set_lock_state(lock, LockState.UNLOCKED)


try:
    import filelock as fl
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
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
        name = ConfigContext.get().name
        if name is not None:
            project_name = normalize_name(name)
            self.base_path = os.path.join(self.base_path, project_name)
        self.locks: dict[Lockable, fl.BaseFileLock] = {}

        os.makedirs(self.base_path, exist_ok=True)

    def acquire(self, lock: Lockable):
        if lock not in self.locks:
            lock_path = self.lock_path(lock)
            self.locks[lock] = fl.FileLock(lock_path)

        f_lock = self.locks[lock]
        if not f_lock.is_locked:
            self.logger.info(f"Locking '{lock}'")
        f_lock.acquire()
        self.set_lock_state(lock, LockState.LOCKED)

    def release(self, lock: Lockable):
        if lock not in self.locks:
            raise LockError(f"No lock '{lock}' found.")

        f_lock = self.locks[lock]
        f_lock.release()

        if not f_lock.is_locked:
            self.logger.info(f"Unlocking '{lock}'")
            os.remove(f_lock.lock_file)
            del self.locks[lock]
            self.set_lock_state(lock, LockState.UNLOCKED)

    def lock_path(self, lock: Lockable):
        if isinstance(lock, Stage):
            return os.path.join(self.base_path, lock.name + ".lock")
        elif isinstance(lock, str):
            return os.path.join(self.base_path, lock + ".lock")
        else:
            raise NotImplementedError(
                f"Can't lock object of type '{type(lock).__name__}'"
            )


try:
    import kazoo
    from kazoo.client import KazooClient, KazooState
    from kazoo.recipe.lock import Lock as KazooLock
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
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
            atexit.register(lambda: self.close())
        self.client.add_listener(self._lock_listener)

        self.locks: dict[Lockable, KazooLock] = {}
        self.base_path = "/pipedag/locks/"
        config = ConfigContext.get()
        if config.name is not None:
            project_name = normalize_name(config.name)
            self.base_path += project_name + "/"

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        client = KazooClient(**config)
        return cls(client)

    def close(self):
        self.client.stop()
        self.client.close()

    def acquire(self, lock: Lockable):
        zk_lock = self.client.Lock(self.lock_path(lock))
        self.logger.info(f"Locking '{lock}'")
        if not zk_lock.acquire():
            raise LockError(f"Failed to acquire lock '{lock}'")
        self.locks[lock] = zk_lock
        self.set_lock_state(lock, LockState.LOCKED)

    def release(self, lock: Lockable):
        if lock not in self.locks:
            raise LockError(f"No lock '{lock}' found.")

        self.logger.info(f"Unlocking '{lock}'")
        self.locks[lock].release()
        del self.locks[lock]
        self.set_lock_state(lock, LockState.UNLOCKED)

    def lock_path(self, lock: Lockable):
        if isinstance(lock, Stage):
            return self.base_path + lock.name
        elif isinstance(lock, str):
            return self.base_path + lock
        else:
            raise NotImplementedError(
                f"Can't lock object of type '{type(lock).__name__}'"
            )

    def _lock_listener(self, state):
        if state == KazooState.SUSPENDED:
            for lock in self.locks.keys():
                self.set_lock_state(lock, LockState.UNCERTAIN)
        elif state == KazooState.LOST:
            for lock in self.locks.keys():
                self.set_lock_state(lock, LockState.INVALID)
        elif state == KazooState.CONNECTED:
            for lock in self.locks.keys():
                self.set_lock_state(lock, LockState.LOCKED)
