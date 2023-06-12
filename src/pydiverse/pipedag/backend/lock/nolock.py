from __future__ import annotations

from pydiverse.pipedag.backend.lock.base import BaseLockManager, Lockable, LockState


class NoLockManager(BaseLockManager):
    """Non locking lock manager (oxymoron)

    This lock manager doesn't do any locking and only serves as a placeholder
    for an actual lock manager for testing something locally.

    .. WARNING::
        DON'T USE THIS IN A PRODUCTION ENVIRONMENT. A LOCK MANAGER IS
        ESSENTIAL TO PREVENT DATA CORRUPTION.
    """

    @property
    def supports_stage_level_locking(self):
        return True

    def acquire(self, lockable: Lockable):
        self.set_lock_state(lockable, LockState.LOCKED)

    def release(self, lockable: Lockable):
        self.set_lock_state(lockable, LockState.UNLOCKED)
