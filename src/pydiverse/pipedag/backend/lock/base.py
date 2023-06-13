from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from contextlib import contextmanager
from enum import Enum
from typing import Callable, Union

import structlog

from pydiverse.pipedag import Stage
from pydiverse.pipedag.errors import LockError
from pydiverse.pipedag.util import Disposable


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


Lockable = Union[Stage, str]  # noqa: UP007
LockStateListener = Callable[[Lockable, LockState, LockState], None]


class BaseLockManager(Disposable, ABC):
    """Lock Manager base class

    A lock manager is responsible for acquiring and releasing locks on
    stage. This is necessary to prevent two flows from accessing the
    same stage at the same time (which would lead to corrupted data).
    """

    def __init__(self):
        self.logger = structlog.get_logger(logger_name=type(self).__name__)

        self.state_listeners = set()
        self.lock_states = {}
        self.__lock_state_lock = threading.Lock()

    @contextmanager
    def __call__(self, lock: Lockable):
        self.acquire(lock)
        try:
            yield
        finally:
            self.release(lock)

    def dispose(self):
        self.release_all()
        super().dispose()

    @property
    @abstractmethod
    def supports_stage_level_locking(self):
        """
        Flag indicating if locking is supported on stage level, or only on
        instance level.
        """

    @abstractmethod
    def acquire(self, lockable: Lockable):
        """Acquires a lock to access a given object"""

    @abstractmethod
    def release(self, lockable: Lockable):
        """Releases a previously acquired lock"""

    def release_all(self):
        """Releases all acquired locks"""
        locks = list(self.lock_states.items())
        for lock, _state in locks:
            self.release(lock)

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

    def set_lock_state(self, lockable: Lockable, new_state: LockState):
        """Update the state of a lock

        Function used by lock implementations to update the state of a
        lock. If appropriate, listeners will be informed about this change.
        """
        with self.__lock_state_lock:
            if lockable not in self.lock_states:
                self.lock_states[lockable] = new_state
                for listener in self.state_listeners:
                    listener(lockable, LockState.UNLOCKED, new_state)
            else:
                old_state = self.lock_states[lockable]
                self.lock_states[lockable] = new_state
                if old_state != new_state:
                    for listener in self.state_listeners:
                        listener(lockable, old_state, new_state)

            if new_state == LockState.UNLOCKED:
                del self.lock_states[lockable]

    def get_lock_state(self, lockable: Lockable) -> LockState:
        """Returns the state of a lock"""
        with self.__lock_state_lock:
            return self.lock_states.get(lockable, LockState.UNLOCKED)


class InstanceLevelLockManager(BaseLockManager, ABC):
    """
    Base class for a lock manager that only supports locking on instance level
    """

    def __init__(self):
        super().__init__()
        self.__locks_lock = threading.Lock()
        self.__locks: set[Lockable] = set()

    @property
    def supports_stage_level_locking(self) -> bool:
        return False

    def acquire(self, lockable: Lockable):
        with self.__locks_lock:
            if len(self.__locks):
                self._acquire_instance_lock()

            if lockable in self.__locks:
                raise LockError("Lock already acquired.")
            self.__locks.add(lockable)

        self.set_lock_state(lockable, LockState.LOCKED)

    def release(self, lockable: Lockable):
        with self.__locks_lock:
            if lockable not in self.__locks:
                raise LockError(f"No lock '{lockable}' found.")
            self.__locks.remove(lockable)

            if len(self.__locks):
                self._release_instance_lock()

        self.set_lock_state(lockable, LockState.UNLOCKED)

    def release_all(self):
        with self.__locks_lock:
            self.__locks.clear()
            self.lock_states.clear()
            self._release_instance_lock()

    @abstractmethod
    def _acquire_instance_lock(self):
        """Acquires a lock on the instance level"""

    @abstractmethod
    def _release_instance_lock(self):
        """Releases a previous lock on the instance level"""
