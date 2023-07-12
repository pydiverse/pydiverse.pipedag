from __future__ import annotations

import hashlib
import threading
import time
import warnings
from abc import ABC, abstractmethod
from typing import Any

import sqlalchemy as sa

from pydiverse.pipedag import ConfigContext, Stage
from pydiverse.pipedag.backend.lock.base import BaseLockManager, Lockable, LockState
from pydiverse.pipedag.backend.table.sql.ddl import (
    CreateSchema,
    Schema,
)
from pydiverse.pipedag.errors import LockError


class DatabaseLockManager(BaseLockManager):
    """Lock manager based on database locking mechanisms

    Many databases provide some kind of locking mechanism. Depending on the specific
    database technology, this allows us to implement locking on either the schema
    level (where each stage can be locked and unlocked individually), or only on
    the instance level (where the entire instance including all stages get locked and
    unlocked together).

    This lock manager uses the same database as the ``SQLTableStore``.
    No other configuration in the ``args`` section of the config file is required.

    We currently support the following databases:
    PostgreSQL, Microsoft SQL Server, IBM DB2.
    """

    __registered_dialects: dict[str, type[DatabaseLockManager]] = {}
    _dialect_name: str

    def __new__(cls, engine: sa.Engine, *args, **kwargs):
        if cls != DatabaseLockManager:
            return super().__new__(cls)

        # If calling DatabaseLockManager(engine), then we want to dynamically
        # instantiate the correct dialect specific subclass based on the engine dialect.
        dialect = engine.dialect.name
        dialect_specific_cls = DatabaseLockManager.__registered_dialects.get(
            dialect, cls
        )
        return super(DatabaseLockManager, dialect_specific_cls).__new__(
            dialect_specific_cls
        )

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
        create_lock_schema = not table_store.avoid_drop_create_schema

        return cls(engine, instance_id, lock_schema, create_lock_schema)

    def __init__(
        self,
        engine: sa.Engine,
        instance_id: str,
        lock_schema: Schema | None = None,
        create_lock_schema: bool = True,
    ):
        super().__init__()

        self.engine = engine
        self.instance_id = instance_id
        self.lock_schema = lock_schema
        self.create_lock_schema = create_lock_schema

        # Stage level locking
        self.connection = None
        self.locks: dict[Lockable, Lock] = {}

        # Instance level locking
        self.il_lock: Lock | None = None
        self.il_locks: set[Lockable] = set()
        self.__il_threading_lock = threading.Lock()

        # Prepare database
        self.prepare()

    def __init_subclass__(cls, **kwargs):
        # Whenever a new subclass if DatabaseLockManager is defined, it must contain the
        # `_dialect_name` attribute. This allows us to dynamically instantiate it
        # when DatabaseLockManager SQLTableStore(engine, ...) based on the dialect
        # of the engine (see __new__).
        dialect_name = getattr(cls, "_dialect_name", None)
        if dialect_name is None:
            raise ValueError(
                "All subclasses of DatabaseLockManager must have a `_dialect_name`"
                f"attribute. But {cls.__name__}._dialect_name is None."
            )

        if dialect_name in DatabaseLockManager.__registered_dialects:
            warnings.warn(
                f"Already registered a DatabaseLockManager for dialect {dialect_name}"
            )
        DatabaseLockManager.__registered_dialects[dialect_name] = cls

    def prepare(self):
        pass

    def _prepare_create_lock_schema(self):
        # Create the lock schema
        if not self.create_lock_schema:
            return

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
        raise NotImplementedError

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

                if lockable in self.il_locks:
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

                if lockable not in self.il_locks:
                    raise LockError(f"No lock '{lockable}' found.")
                self.il_locks.remove(lockable)
                self.set_lock_state(lockable, LockState.UNLOCKED)

                if len(self.il_locks) == 0:
                    self.logger.info("Unlocking at instance level")
                    self.il_lock.release()

    def get_lock_object(self, lockable: Lockable):
        raise NotImplementedError(f"Dialect '{self.engine.dialect.name}' not supported")

    def get_instance_level_lock(self):
        raise NotImplementedError(f"Dialect '{self.engine.dialect.name}' not supported")

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
        result = self._connection.exec_driver_sql(
            f"SELECT pg_catalog.pg_advisory_lock({self._id})"
        ).scalar()
        result = False if result is False else True
        self._locked = result
        return result

    def release(self) -> bool:
        result = self._connection.exec_driver_sql(
            f"SELECT pg_catalog.pg_advisory_unlock({self._id})"
        ).scalar()
        self._locked = False
        return False if result is False else True

    @property
    def locked(self) -> bool:
        return self._locked


class PostgresLockManager(DatabaseLockManager):
    _dialect_name = "postgresql"
    supports_stage_level_locking = True

    def get_lock_object(self, lockable: Lockable):
        if self.connection is None:
            self.connection = self.engine.connect()

        name = self.lock_name(lockable)
        return PostgresLock(name, self.connection)

    def dispose(self):
        self.release_all()
        self.connection.close()
        super().dispose()


class MSSqlLock(Lock):
    """
    Locking based on application resource locks.
    https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-ver15
    """

    def __init__(self, name: str, connection: sa.Connection):
        self.name = name
        self._connection = connection
        self._locked = False

    def acquire(self) -> bool:
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
        self._locked = False
        return result

    @property
    def locked(self) -> bool:
        return self._locked


class MSSqlLockManager(DatabaseLockManager):
    _dialect_name = "mssql"
    supports_stage_level_locking = True

    def get_lock_object(self, lockable: Lockable):
        if self.connection is None:
            self.connection = self.engine.connect()

        name = self.lock_name(lockable)
        return MSSqlLock(name, self.connection)


class DB2Lock(Lock):
    """
    Locking based on 'LOCK TABLE' statements.
    https://www.ibm.com/docs/en/db2-for-zos/13?topic=statements-lock-table

    These locks can only be released by committing a transaction. Therefor it is
    not suitable for fine grain locking.
    """

    def __init__(self, table: str, schema: str, engine: sa.Engine):
        self._engine = engine
        self._connection = None
        self._locked = False

        self.table = table
        self.schema = schema

    def acquire(self) -> bool:
        table = self._engine.dialect.identifier_preparer.quote(self.table)
        schema = self._engine.dialect.identifier_preparer.format_schema(self.schema)

        # CREATE TABLE IF NOT EXISTS
        with self._engine.begin() as conn:
            conn.exec_driver_sql(
                f"""
                BEGIN
                    DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
                    EXECUTE IMMEDIATE 'CREATE TABLE {schema}.{table} (x int)';
                END
                """
            )

        # LOCK TABLE
        if self._connection is None:
            self._connection = self._engine.connect()
            self._connection.begin()

        self._connection.exec_driver_sql(
            f"LOCK TABLE {schema}.{table} IN EXCLUSIVE MODE"
        )

        self._locked = True
        return True

    def release(self) -> bool:
        if self._connection is None:
            return False

        self._connection.close()
        self._locked = False
        return True

    @property
    def locked(self) -> bool:
        return self._locked


class DB2LockManager(DatabaseLockManager):
    _dialect_name = "ibm_db_sa"
    supports_stage_level_locking = False

    def prepare(self):
        self._prepare_create_lock_schema()

    def get_instance_level_lock(self):
        return DB2Lock("instance_lock", self.lock_schema.get(), self.engine)
