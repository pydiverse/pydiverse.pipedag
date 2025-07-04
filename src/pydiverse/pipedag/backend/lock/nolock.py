# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

from pydiverse.pipedag.backend.lock.base import BaseLockManager, Lockable, LockState


class NoLockManager(BaseLockManager):
    """
    This lock manager doesn't do any locking and only serves as a placeholder
    for an actual lock manager for testing something locally.

    .. Warning::
        This lock manager is not intended for use in a production environment.
        Using a lock manager is essential for preventing data corruption.
    """

    @property
    def supports_stage_level_locking(self):
        return True

    def acquire(self, lockable: Lockable):
        self.set_lock_state(lockable, LockState.LOCKED)

    def release(self, lockable: Lockable):
        self.set_lock_state(lockable, LockState.UNLOCKED)
