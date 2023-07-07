from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Any

from pydiverse.pipedag import ConfigContext, Stage
from pydiverse.pipedag.backend.lock.base import BaseLockManager, Lockable, LockState
from pydiverse.pipedag.errors import LockError
from pydiverse.pipedag.util import normalize_name, requires

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

    :param base_path:
        A path to a folder where the lock files should get stored.
        To differentiate between different instances, the ``instance_id`` will
        automatically be appended to the provided path.

    .. _filelock documentation: https://py-filelock.readthedocs.io/en/latest/index.html
    """

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        instance_id = normalize_name(ConfigContext.get().instance_id)
        base_path = Path(config["base_path"]) / instance_id
        return cls(base_path)

    def __init__(self, base_path: str | Path):
        super().__init__()
        self.base_path = Path(base_path).absolute()
        self.locks: dict[Lockable, fl.BaseFileLock] = {}

        os.makedirs(self.base_path, exist_ok=True)

    @property
    def supports_stage_level_locking(self):
        return True

    def acquire(self, lockable: Lockable):
        if lockable not in self.locks:
            lock_path = self.lock_path(lockable)
            self.locks[lockable] = fl.FileLock(lock_path)

        lock = self.locks[lockable]
        if not lock.is_locked:
            self.logger.info(f"Locking '{lockable}'")
        lock.acquire()
        self.set_lock_state(lockable, LockState.LOCKED)

    def release(self, lockable: Lockable):
        if lockable not in self.locks:
            raise LockError(f"No lock '{lockable}' found.")

        lock = self.locks[lockable]
        lock.release()
        if not lock.is_locked:
            self.logger.info(f"Unlocking '{lockable}'")
            del self.locks[lockable]
            self.set_lock_state(lockable, LockState.UNLOCKED)

    def lock_path(self, lock: Lockable) -> Path:
        if isinstance(lock, Stage):
            return self.base_path / (lock.name + ".lock")
        elif isinstance(lock, str):
            return self.base_path / (lock + ".lock")
        else:
            raise NotImplementedError(
                f"Can't lock object of type '{type(lock).__name__}'"
            )
