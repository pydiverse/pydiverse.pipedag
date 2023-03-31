from __future__ import annotations

from abc import ABC, ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Generic

import structlog
from typing_extensions import Self

from pydiverse.pipedag._typing import StoreT, T
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.materialize.cache import CacheManager
from pydiverse.pipedag.materialize.container import RawSql, Table
from pydiverse.pipedag.materialize.metadata import (
    LazyTableMetadata,
    RawSqlMetadata,
    TaskMetadata,
)
from pydiverse.pipedag.materialize.util import compute_cache_key
from pydiverse.pipedag.util import Disposable, requires

if TYPE_CHECKING:
    from pydiverse.pipedag import Stage
    from pydiverse.pipedag.materialize.core import MaterializingTask, TaskInfo


class _TableStoreMeta(ABCMeta):
    """TableStore Metaclass

    Add the `_REGISTERED_TABLES`, `_M_TABLE_CACHE` and `_R_TABLE_CACHE`
    attributes to all table stores.
    """

    def __new__(mcs, name, bases, attrs, **kwargs):
        cls = super().__new__(mcs, name, bases, attrs, **kwargs)

        cls._REGISTERED_TABLES = []
        cls._M_TABLE_CACHE = {}
        cls._R_TABLE_CACHE = {}

        return cls


class BaseTableStore(Disposable, metaclass=_TableStoreMeta):
    """Table store base class

    The table store is responsible for storing and retrieving various types
    of tabular data. Additionally, it also has to manage all task metadata,
    This includes storing it, but also cleaning up stale metadata.

    A store must use a table's name (`table.name`) and stage (`table.stage`)
    as the primary keys for storing and retrieving it. This means that
    two different `Table` objects can be used to store and retrieve the same
    data as long as they have the same name and stage.

    The same is also true for the task metadata where the task `stage`,
    `version` and `cache_key` act as the primary keys (those values are
    stored both in the task object and the metadata object).

    To implement the stage transaction and commit mechanism, a technique
    called schema swapping is used:

    All outputs from materializing tasks get materialized into a temporary
    empty schema (`stage.transaction_name`) and only if all tasks have
    finished running *successfully* you swap the 'base schema' (original stage,
    or cache) with the 'transaction schema'. This is usually done by renaming
    them.

    """

    _REGISTERED_TABLES: list[TableHook]
    _M_TABLE_CACHE: dict[type, TableHook]
    _R_TABLE_CACHE: dict[type, TableHook]

    def __init__(self):
        self.logger = structlog.get_logger(type(self).__name__)

    def setup(self):
        """Setup function

        This function gets called at the beginning of a flow run.
        Unlike the __init__ method, a lock is acquired before
        the setup method gets called to prevent race conditions.
        """

    @classmethod
    def register_table(cls, *requirements: Any):
        """Decorator to register a `TableHook`

        Each table store should be able to handle tables of various different
        types (e.g. a pandas dataframe, a sqlalchemy select statement, a
        pydiverse transform table, ...). To add table type specific logic to
        a table store so called TableHooks are used.

        This decorator is used to register such a hook:
        ::
            try:
                from library import x
            except ImportError as e:
                warnings.warn(str(e), ImportWarning)
                x = None

            @TableStore.register_table(x)
            def XTableHook(TableHook):
                ...

        :param requirements: The requirements which must be satisfied to register
            the decorated class.
        """

        def decorator(hook_cls):
            if not all(requirements):
                return requires(
                    False,
                    Exception(f"Not all requirements met for {hook_cls.__name__}"),
                )(hook_cls)

            # Register the hook
            cls._REGISTERED_TABLES.append(hook_cls)
            cls._M_TABLE_CACHE.clear()
            cls._R_TABLE_CACHE.clear()
            return hook_cls

        return decorator

    def get_m_table_hook(self: Self, type_: type[T]) -> TableHook[Self]:
        """Get a table hook that can materialize the specified type"""
        if type_ in self._M_TABLE_CACHE:
            return self._M_TABLE_CACHE[type_]
        for hook in self._REGISTERED_TABLES:
            if hook.can_materialize(type_):
                self._M_TABLE_CACHE[type_] = hook
                return hook
        raise TypeError(f"Can't materialize Table with underlying type {type_}")

    def get_r_table_hook(self: Self, type_: type[T]) -> TableHook[Self]:
        """Get a table hook that can retrieve the specified type"""
        if type_ in self._R_TABLE_CACHE:
            return self._R_TABLE_CACHE[type_]
        for hook in self._REGISTERED_TABLES:
            if hook.can_retrieve(type_):
                self._R_TABLE_CACHE[type_] = hook
                return hook
        raise TypeError(f"Can't retrieve Table as type {type_}")

    # Stage

    @abstractmethod
    def init_stage(self, stage: Stage):
        """Initialize a stage transaction

        When working with schema swapping:

        Ensures that the base schema exists (but doesn't clear it) and that
        the transaction schema exists and is empty.
        """

    @abstractmethod
    def commit_stage(self, stage: Stage):
        """Commit the stage

        When using schema swapping:

        After the schema swap the contents of the base schema should be in the
        transaction schema, and the contents of the transaction schema in
        the base schema.

        Additionally, the metadata associated with the transaction schema should
        replace the metadata of the base schema. The latter can be discarded.
        """

    # Materialize

    def store_table(self, table: Table, task: MaterializingTask, task_info: TaskInfo):
        """Stores a table in the associated transaction stage

        The store must convert the table object (`table.obj`) to the correct
        internal type. This means, that in some cases it first has to
        evaluate a lazy object. For example: if a sql based table store
        receives a sql query to store, it has to execute it first.

        The implementation details of this get handled by the registered
        TableHooks.
        """
        _ = task  # not needed in this kind of store_table, yet
        hook = self.get_m_table_hook(type(table.obj))
        hook.materialize(self, table, table.stage.transaction_name, task_info)

    def execute_raw_sql(self, raw_sql: RawSql):
        """Executed raw SQL statements in the associated transaction stage

        This method is overridden by actual table stores that can handle raw SQL.
        """

        raise NotImplementedError(
            "This table store does not support executing raw sql statements"
        )

    def store_table_lazy(
        self,
        table: Table,
        task: MaterializingTask,
        task_info: TaskInfo,
    ):
        """Lazily stores a table in the associated commit stage

        The same as `store_table()`, with the difference being that if the
        table object represents a lazy table / query, the store first checks
        if the same query with the same input (based on `table.cache_key`)
        has already been executed before. If yes, instead of evaluating
        the query, it just copies the previous result to the commit stage.

        Used when `lazy = True` is set for a materializing task.
        """
        _ = task
        try:
            hook = self.get_m_table_hook(type(table.obj))
            query_str = hook.lazy_query_str(self, table.obj)
            query_hash = compute_cache_key("LAZY-TABLE", query_str)
        except TypeError:
            # This table type doesn't provide a query string
            # -> Fallback to default implementation
            return self.store_table(table, task, task_info)

        table_cache_info = CacheManager.lazy_table_cache_lookup(
            self, task_info.task_cache_info, table, query_hash
        )
        if not table_cache_info.is_cache_valid():
            RunContext.get().stage_output_cache_invalid(task.stage)
            self.store_table(table, task, task_info)

        # At this point we MUST also update the cache info, so that any downstream
        # tasks get invalidated if the sql query string changed.
        table.cache_key = CacheManager.lazy_table_cache_key(
            task_info.task_cache_info.get_task_cache_key(), query_hash
        )

    def store_raw_sql(
        self, raw_sql: RawSql, task: MaterializingTask, task_info: TaskInfo
    ):
        """Lazily stores a table in the associated commit stage

        The same as `store_table()`, with the difference being that the store first
        checks if the same query with the same input (based on `raw_sql.cache_key`)
        has already been executed before. If yes, instead of evaluating
        the query, it just copies the previous result to the commit stage.
        """
        _ = task
        query_hash = compute_cache_key("RAW-SQL", raw_sql.sql)

        table_cache_info = CacheManager.raw_sql_cache_lookup(
            self, task_info.task_cache_info, raw_sql, query_hash
        )
        if not table_cache_info.is_cache_valid():
            RunContext.get().stage_output_cache_invalid(task.stage)
            prev_tables = self.list_tables(raw_sql.stage, include_everything=True)
            self.execute_raw_sql(raw_sql)
            post_tables = self.list_tables(raw_sql.stage, include_everything=True)
            table_cache_info.store_raw_sql_metadata(self, prev_tables, post_tables)

        # At this point we MUST also update the cache info, so that any downstream
        # tasks get invalidated if the sql query string changed.
        raw_sql.cache_key = CacheManager.lazy_table_cache_key(
            task_info.task_cache_info.get_task_cache_key(), query_hash
        )

    @abstractmethod
    def copy_table_to_transaction(self, table: Table):
        """Copy a table from the base stage to the transaction stage

        This operation MUST not remove the table from the base stage store
        or modify it in any way.

        :raises CacheError: if the table can't be found in the cache
        """

    @abstractmethod
    def copy_lazy_table_to_transaction(self, metadata: LazyTableMetadata, table: Table):
        """Copy the lazy table identified by the metadata to the transaction stage of
        table.

        This operation MUST not remove the table from the base stage or modify
        it in any way.

        :raises CacheError: if the lazy table can't be found
        """

    def copy_raw_sql_tables_to_transaction(
        self, metadata: RawSqlMetadata, stage: Stage
    ):
        """Copy all tables identified by the metadata as generated by raw
        SQL statements to the transaction stage.

        This operation MUST not remove the table from the base stage or modify
        it in any way.

        :raises CacheError: if the lazy table can't be found
        """
        raise NotImplementedError(
            "This table store does not support executing raw sql statements"
        )

    @abstractmethod
    def delete_table_from_transaction(self, table: Table):
        """Delete a table from the transaction

        If the table doesn't exist in the transaction stage, fail silently.
        """

    # Dematerialize

    def retrieve_table_obj(self, table: Table, as_type: type[T]) -> T:
        """Loads a table from the store

        Retrieves the table from the store, converts it to the correct
        If the stage hasn't yet been committed, the table must be retrieved
        from the transaction, else it must be retrieved from the committed
        stage.

        :raises TypeError: if the retrieved table can't be converted to
            the requested type.
        """

        if as_type is None:
            raise TypeError(
                "Missing 'as_type' argument. You must specify a type to be able "
                "to dematerialize a Table."
            )

        hook = self.get_r_table_hook(as_type)
        return hook.retrieve(self, table, table.stage.current_name, as_type)

    # Metadata

    @abstractmethod
    def store_task_metadata(self, metadata: TaskMetadata, stage: Stage):
        """Stores the metadata of a task

        The metadata must always be stored in such a way that it is
        associated with the transaction. Only after a stage has been
        committed, should it be associated with the base stage / cache.
        """

    @abstractmethod
    def retrieve_task_metadata(
        self, task: MaterializingTask, input_hash: str, cache_fn_hash: str
    ) -> TaskMetadata:
        """Retrieve a task's metadata from the store

        :raises CacheError: if no metadata for this task can be found.
        """

    # Lazy Table Metadata

    @abstractmethod
    def store_lazy_table_metadata(self, metadata: LazyTableMetadata):
        """Stores the metadata of a lazy table

        The metadata must always be stored in such a way that it is
        associated with the transaction. Only after a stage has been
        committed, should it be associated with the base stage / cache.
        """

    @abstractmethod
    def retrieve_lazy_table_metadata(
        self, query_hash: str, task_hash: str, stage: Stage
    ) -> LazyTableMetadata:
        """Retrieve a lazy table's metadata from the store

        :param query_hash: A hash of the query that produced this lazy table
        :param task_hash: The hash of the task for which we want to retrieve this
            metadata. This can be used to retrieve the lazy table metadata produced
            by the same task in a previous run, if the current task is still cache
            valid.
        :param stage: The stage in which this lazy table should be.
        :return: The metadata.

        :raises CacheError: if not metadata that matches the provided inputs was found.
        """

    def store_raw_sql_metadata(self, metadata: RawSqlMetadata):
        """Stores the metadata of raw SQL statements

        The metadata must always be stored in such a way that it is
        associated with the transaction. Only after a stage has been
        committed, should it be associated with the base stage / cache.
        """
        raise NotImplementedError(
            "This table store does not support executing raw sql statements"
        )

    def retrieve_raw_sql_metadata(
        self, query_hash: str, task_hash: str, stage: Stage
    ) -> RawSqlMetadata:
        """Retrieve raw SQL metadata from the store

        :param query_hash: A hash of the query that produced this raw sql object
        :param task_hash: The hash of the task for which we want to retrieve this
            metadata. This can be used to retrieve the raw sql metadata produced
            by the same task in a previous run, if the current task is still cache
            valid.
        :param stage: The stage associated with the raw sql object.
        :return: The metadata.

        :raises CacheError: if not metadata that matches the provided inputs was found.
        """
        raise NotImplementedError(
            "This table store does not support executing raw sql statements"
        )

    # Utility

    @abstractmethod
    def list_tables(self, stage: Stage, *, include_everything=False) -> list[str]:
        """
        List all tables that were generated in a stage.

        It may also include other objects database objects like views, stored
        procedures, functions, etc. which makes the name `list_tables` too specific.
        But the predominant idea is that tasks produce tables in stages and thus the
        storyline of callers is much nicer to read. In the end we might need everything
        to recover the full cache output which was produced by a RawSQL statement
        (we want to be compatible with legacy sql code as a starting point).

        :param stage: the stage
        :param include_everything: If True, we might include stored procedures,
            functions and other database objects that have a schema associated name.
        :return: list of tables [and other objects]
        """


class TableHook(Generic[StoreT], ABC):
    """Base class to define how to handle a specific table

    For more information, take a look at the `BaseTableStore.register_table`
    documentation.
    """

    __slots__ = ()

    @classmethod
    @abstractmethod
    def can_materialize(cls, type_: type) -> bool:
        """
        Return `True` if this hook can materialize a table with the specified
        underlying type. If `True` is returned, the `materialize` method
        MUST be implemented for the type.
        """

    @classmethod
    @abstractmethod
    def can_retrieve(cls, type_: type) -> bool:
        """
        Return `True` if this hook can retrieve a table from the store
        and convert it to the specified type. If `True` is returned, the
        `retrieve` method MUST be implemented for the type.
        """

    @classmethod
    @abstractmethod
    def materialize(
        cls,
        store: StoreT,
        table: Table,
        stage_name: str,
        task_info: TaskInfo,
    ) -> None:
        """Materialize a table object

        :param store: The store which called this method
        :param table: The table that should be materialized
        :param stage_name: The name of the stage in which the table should
            be stored - can either be `stage.name` or `stage.transaction_name`.
        :param task_info: Information about task execution
        """

    @classmethod
    @abstractmethod
    def retrieve(
        cls, store: StoreT, table: Table, stage_name: str, as_type: type[T]
    ) -> T:
        """Retrieve a table from the store

        :param store: The store in which the table is stored
        :param table: The table which should get retrieved
        :param stage_name: The name of the stage from which te table should
            be retrieved
        :param as_type: The type as which the table is to be retrieved
        :return: The retrieved table (converted to the correct type)
        """

    @classmethod
    def auto_table(cls, obj: T) -> Table[T]:
        """Wrap an object inside a `Table`

        Given an object that can be materialized by this hook, produces a
        Table instance that may contain additional metadata. This is useful
        to enable automatic name propagation.

        :param obj: The object which should get wrapped inside a `Table`
        :return: The `Table` object which wraps `obj`
        """
        return Table(obj)

    @classmethod
    def lazy_query_str(cls, store: StoreT, obj) -> str:
        """String that represents the associated object

        Can either be a literal query string (e.g. SQL query), or a string that
        uniquely represents the query in some other way. Used for computing
        the cache key for materializing tasks with `lazy=True`.

        :raises TypeError: if the type doesn't support lazy queries.
        """
        raise TypeError(f"Lazy query not supported with object of type {type(obj)}")
