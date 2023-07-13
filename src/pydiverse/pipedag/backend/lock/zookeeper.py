from __future__ import annotations

import atexit
import warnings
from typing import Any

from pydiverse.pipedag import ConfigContext, Stage
from pydiverse.pipedag.backend.lock.base import BaseLockManager, Lockable, LockState
from pydiverse.pipedag.errors import DisposedError, LockError
from pydiverse.pipedag.util import normalize_name, requires

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
    globally synchronous`_. The advantage of this approach is that we it is highly
    reliable and that in case our flow crashes, the acquired locks automatically
    get released (the locks are ephemeral).

    Config File
    -----------
    All arguments in the ``args`` section get passed as-is to the initializer
    of :py:class:`kazoo.client.KazooClient`. Some useful arguments include:

    :param hosts:
        Comma separated list of hosts to connect.

    .. _fully distributed locks that are globally synchronous:
        https://zookeeper.apache.org/doc/r3.1.2/recipes.html#sc_recipes_Locks
    """

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        client = KazooClient(**config)
        instance_id = normalize_name(ConfigContext.get().instance_id)
        base_path = f"/pipedag/locks/{instance_id}/"
        return cls(client, base_path)

    def __init__(self, client: KazooClient, base_path: str):
        super().__init__()

        self.client = client
        self.base_path = base_path
        if not self.client.connected:
            self.client.start()
            atexit.register(self.__atexit)
        self.client.add_listener(self._lock_listener)

        self.locks: dict[Lockable, KazooLock] = {}

    def __atexit(self):
        try:
            self.dispose()
        except DisposedError:
            pass

    def dispose(self):
        self.release_all()
        self.client.stop()
        self.client.close()
        self.lock_states.clear()
        self.locks.clear()
        super().dispose()

    @property
    def supports_stage_level_locking(self):
        return True

    def acquire(self, lockable: Lockable):
        lock = self.client.Lock(self.lock_path(lockable))
        self.logger.info(f"Locking '{lockable}'", base_path=self.base_path)
        if not lock.acquire():
            raise LockError(f"Failed to acquire lock '{lockable}'")
        self.locks[lockable] = lock
        self.set_lock_state(lockable, LockState.LOCKED)

    def release(self, lockable: Lockable):
        if lockable not in self.locks:
            raise LockError(f"No lock '{lockable}' found.")

        self.logger.info(f"Unlocking '{lockable}'", base_path=self.base_path)
        self.locks[lockable].release()
        del self.locks[lockable]
        self.set_lock_state(lockable, LockState.UNLOCKED)

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
