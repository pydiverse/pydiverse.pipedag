from __future__ import annotations

import hashlib
import threading
import time
from abc import ABC, abstractmethod
from typing import Any

import sqlalchemy as sa

from pydiverse.pipedag import ConfigContext, Stage
from pydiverse.pipedag.backend.lock.base import BaseLockManager, Lockable, LockState
from pydiverse.pipedag.backend.table.util import engine_dispatch
from pydiverse.pipedag.backend.table.util.sql_ddl import CreateSchema, Schema
from pydiverse.pipedag.errors import LockError


class DatabaseLockManager(BaseLockManager):
    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        assert (
            len(config) == 0
        ), "DatabaseLockManager doesn't take any additional arguments"

        cfg_context = ConfigContext.get()
        return cls.init_from_config_context(cfg_context)

    @classmethod
    def init_from_config_context(cls, config_context: ConfigContext):
        from pydiverse.pipedag.backend.table import SQLTableStore

        table_store = config_context.store.table_store
        if not isinstance(table_store, SQLTableStore):
            raise TypeError("Table Store must be a subclass of SQLTableStore")

        engine = table_store.get_engine_for_locking()
        instance_id = config_context.instance_id
        lock_schema = table_store.get_lock_schema()

        return cls(engine, instance_id, lock_schema)

    def __init__(
        self, engine: sa.Engine, instance_id: str, lock_schema: Schema | None = None
    ):
        super().__init__()

        self.engine = engine
        self.instance_id = instance_id
        self.lock_schema = lock_schema

        # Stage level locking
        self.connection = None
        self.locks: dict[Lockable, Lock] = {}

        # Instance level locking
        self.il_lock: Lock | None = None
        self.il_locks: set[Lockable] = set()
        self.__il_threading_lock = threading.Lock()

        # Prepare database
        self.prepare()

    @engine_dispatch
    def prepare(self):
        ...

    @prepare.dialect("mssql")
    def __prepare(self):
        # Create the lock schema
        # If two lock managers do this concurrently, this action might fail. Thus,
        # we retry a second time to ensure the schema exists.
        with self.engine.connect() as conn:
            exception = None
            for _ in range(3):
                try:
                    with conn.begin():
                        conn.execute(CreateSchema(self.lock_schema, if_not_exists=True))
                    break
                except sa.exc.DBAPIError as e:
                    exception = e
                    time.sleep(0.1)
            else:
                raise exception

    def dispose(self):
        self.engine.dispose()
        super().dispose()

    @property
    def supports_stage_level_locking(self):
        return True

    def acquire(self, lockable: Lockable):
        if self.supports_stage_level_locking:
            lock = self.get_lock_object(lockable)
            self.logger.info(f"Locking '{lockable}'")
            if not lock.acquire():
                raise LockError(f"Failed to acquire lock '{lockable}'")
            self.locks[lockable] = lock
            self.set_lock_state(lockable, LockState.LOCKED)
        else:
            with self.__il_threading_lock:
                if len(self.il_locks) == 0:
                    if self.il_lock is None:
                        self.il_lock = self.get_instance_level_lock()
                    self.logger.info("Locking at instance level")
                    if not self.il_lock.acquire():
                        raise LockError(f"Failed to acquire lock '{lockable}'")

                if lockable in self.locks:
                    raise LockError("Already acquired lock")
                self.il_locks.add(lockable)
                self.set_lock_state(lockable, LockState.LOCKED)

    def release(self, lockable: Lockable):
        if self.supports_stage_level_locking:
            if lockable not in self.locks:
                raise LockError(f"No lock '{lockable}' found.")

            self.logger.info(f"Unlocking '{lockable}'")
            self.locks[lockable].release()
            del self.locks[lockable]
            self.set_lock_state(lockable, LockState.UNLOCKED)
        else:
            with self.__il_threading_lock:
                if self.il_lock is None or not self.il_lock.locked:
                    raise LockError("Instance level lock must be acquired first")

                if lockable not in self.locks:
                    raise LockError(f"No lock '{lockable}' found.")
                self.il_locks.remove(lockable)
                self.set_lock_state(lockable, LockState.UNLOCKED)

                if len(self.il_locks) == 0:
                    self.logger.info("Unlocking at instance level")
                    self.il_lock.release()

    @engine_dispatch
    def get_lock_object(self, lockable: Lockable):
        raise NotImplementedError(f"Dialect '{self.engine.dialect}' not supported")

    @get_lock_object.dialect("postgresql")
    def __get_lock_object(self, lockable: Lockable):
        if self.connection is None:
            self.connection = self.engine.connect()

        name = self.lock_name(lockable)
        return PostgresLock(name, self.connection)

    @get_lock_object.dialect("mssql")
    def __get_lock_object(self, lockable: Lockable):
        if self.connection is None:
            self.connection = self.engine.connect()

        name = self.lock_name(lockable)
        database, _ = self.lock_schema.get().split(".")
        return MSSqlLock(name, database, self.connection)

    @engine_dispatch
    def get_instance_level_lock(self):
        raise NotImplementedError(f"Dialect '{self.engine.dialect}' not supported")

    def lock_name(self, lock: Lockable):
        if isinstance(lock, Stage):
            return self.instance_id + "#" + lock.name
        elif isinstance(lock, str):
            return self.instance_id + "#" + lock
        else:
            raise NotImplementedError(
                f"Can't lock object of type '{type(lock).__name__}'"
            )


class Lock(ABC):
    @abstractmethod
    def acquire(self) -> bool:
        """Acquire the lock"""

    @abstractmethod
    def release(self) -> bool:
        """Release the lock"""

    @property
    @abstractmethod
    def locked(self) -> bool:
        """Returns the lock status"""


class PostgresLock(Lock):
    """
    Locking based on advisory locks.
    https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS
    """

    def __init__(self, name: str, connection: sa.Connection):
        self.name = name

        digest = hashlib.sha256(name.encode("utf-8")).digest()
        self._id = int.from_bytes(digest[:8], byteorder="big", signed=True)

        self._connection = connection
        self._locked = False

    def acquire(self) -> bool:
        result = self._connection.execute(
            sa.text(f"SELECT pg_catalog.pg_advisory_lock({self._id})")
        ).scalar()
        result = False if result is False else True
        self._locked = result
        return result

    def release(self) -> bool:
        result = self._connection.execute(
            sa.text(f"SELECT pg_catalog.pg_advisory_unlock({self._id})")
        ).scalar()
        self._locked = False
        return False if result is False else True

    @property
    def locked(self) -> bool:
        return self._locked


class MSSqlLock(Lock):
    """
    Locking based on application resource locks.
    https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-ver15
    """

    def __init__(self, name: str, database: str, connection: sa.Connection):
        self.name = name
        self._database = database
        self._connection = connection
        self._locked = False

    def acquire(self) -> bool:
        self._connection.execute(sa.text(f"USE [{self._database}]"))
        result = self._connection.execute(
            sa.text(
                """
                DECLARE @result int
                EXEC @result =
                    sp_getapplock
                        @Resource = :lock_name,
                        @LockMode = 'Exclusive',
                        @LockOwner = 'Session'
                SELECT @result
                """
            ),
            {"lock_name": self.name},
        ).scalar()
        result = result >= 0
        self._locked = result
        return result

    def release(self) -> bool:
        self._connection.execute(sa.text(f"USE [{self._database}]"))
        result = self._connection.execute(
            sa.text(
                """
                DECLARE @result int
                EXEC @result =
                    sp_releaseapplock
                        @Resource = :lock_name,
                        @LockOwner = 'Session'
                SELECT @result
                """
            ),
            {"lock_name": self.name},
        ).scalar()
        result = result >= 0
        return result

    @property
    def locked(self) -> bool:
        return self._locked
