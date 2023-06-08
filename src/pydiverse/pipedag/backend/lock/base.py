from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from enum import Enum
from typing import Callable, Union

import structlog

from pydiverse.pipedag import Stage
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

    @abstractmethod
    def acquire(self, lock: Lockable):
        """Acquires a lock to access a given object"""

    @abstractmethod
    def release(self, lock: Lockable):
        """Releases a previously acquired lock"""

    def release_all(self):
        """Releases all aquired locks"""
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
            return self.lock_states.get(lock, LockState.UNLOCKED)
