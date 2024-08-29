from __future__ import annotations

import sys
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic

import structlog
from typing_extensions import Self

from pydiverse.pipedag import ConfigContext
from pydiverse.pipedag._typing import T, TableHookResolverT
from pydiverse.pipedag.container import RawSql, Table
from pydiverse.pipedag.context import RunContext, TaskContext
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.cache import TaskCacheInfo, lazy_table_cache_key
from pydiverse.pipedag.materialize.metadata import (
    LazyTableMetadata,
    RawSqlMetadata,
    TaskMetadata,
)
from pydiverse.pipedag.util import Disposable, requires
from pydiverse.pipedag.util.computation_tracing import ComputationTracer
from pydiverse.pipedag.util.hashing import stable_hash

if TYPE_CHECKING:
    from pydiverse.pipedag import Stage
    from pydiverse.pipedag.backend.table.cache.base import BaseTableCache
    from pydiverse.pipedag.materialize.core import MaterializingTask


class TableHookResolver:
    class ClassState:
        def __init__(self):
            self.registered_table_hooks: list[type[TableHook]] = []
            self.m_hook_cache: dict[type, type[TableHook]] = {}
            self.r_hook_cache: dict[type, type[TableHook]] = {}
            self.hook_subclass_cache: dict[type, type[TableHook]] = {}

        def add_hook(self, hook_cls, replace_hooks: list[type[TableHook]] | None):
            # we ignore if replace_hooks are not found since they might be found in
            # base class with lower lookup priority
            if replace_hooks:
                self.registered_table_hooks = [
                    x for x in self.registered_table_hooks if x not in replace_hooks
                ]
            self.registered_table_hooks.append(hook_cls)

        def __repr__(self):
            return "ClassState=" + str(
                {
                    x: getattr(self, x)
                    for x in dir(self)
                    if not x.startswith("_") and x != "add_hook"
                }
            )

    _states: dict[int, ClassState] = {}

    @classmethod
    def _resolver_state(cls: TableHookResolver):
        state = cls._states.get(id(cls))
        if state is None:
            state = cls._states[id(cls)] = cls.ClassState()
        return state

    @classmethod
    def register_table(
        cls: TableHookResolver,
        *requirements: Any,
        replace_hooks: list[type[TableHook]] | None = None,
    ):
        """Decorator to register a `TableHook`

        Each table store should be able to handle tables of various different
        types (e.g. a pandas dataframe, a sqlalchemy select statement, a
        pydiverse transform table, ...). To add table type specific logic to
        a table store so called TableHooks are used.

        This decorator is used to register such a hook::

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
        :param replace_hooks: Takes a list of TableHook classes (e.g. PandasTableHook)
            that should be replaced by the decorated class.
            If None, the new hook is just added to the list of available hooks.

            Details: specifying replace_hooks helps also implicitly because the
            mentioned hooks must have been imported/registered before. If they were
            registered on a base class of the hook resolver (i.e. table store) of this
            call, they will be ignored but hooks registered on subclasses take
            precedence anyway. Please make sure not provide replace hooks registered to
            subclasses of this call's hook resolver. This will fail.
        """

        def decorator(hook_cls):
            if not all(requirements):
                cls.__hooks_with_unmet_requirements.append(
                    hook_cls.__module__ + "." + hook_cls.__qualname__
                )
                return requires(
                    False,
                    Exception(f"Not all requirements met for {hook_cls.__name__}"),
                )(hook_cls)

            cls._resolver_state().add_hook(hook_cls, replace_hooks)
            return hook_cls

        return decorator

    def __all_registered_table_hooks(self) -> Iterable[type[TableHook]]:
        # Walk up the hierarchy of super classes and return each registered
        # table hook in reverse order
        for cls in type(self).__mro__:
            if issubclass(cls, TableHookResolver):
                yield from cls._resolver_state().registered_table_hooks[::-1]  # noqa

    def get_m_table_hook(self: Self, type_: type[T]) -> type[TableHook[Self]]:
        """Get a table hook that can materialize the specified type"""
        if type_ in self._resolver_state().m_hook_cache:
            return self._resolver_state().m_hook_cache[type_]

        for hook in self.__all_registered_table_hooks():
            if hook.can_materialize(type_):
                self._resolver_state().m_hook_cache[type_] = hook
                return hook

        raise TypeError(
            f"Can't materialize Table with underlying type {type_}. "
            + self.__hook_unmet_requirements_message()
        )

    def get_r_table_hook(
        self: Self, type_: type[T] | tuple | dict
    ) -> type[TableHook[Self]]:
        """Get a table hook that can retrieve the specified type"""
        if isinstance(type_, tuple):
            type_ = type_[0]
        elif isinstance(type_, dict):
            type_ = type_["type"]

        if type_ is None:
            raise ValueError("type_ argument can't be None")

        if type_ in self._resolver_state().r_hook_cache:
            return self._resolver_state().r_hook_cache[type_]

        for hook in self.__all_registered_table_hooks():
            if hook.can_retrieve(type_):
                self._resolver_state().r_hook_cache[type_] = hook
                return hook

        raise TypeError(
            f"Can't retrieve Table as type {type_}. "
            + self.__hook_unmet_requirements_message()
        )

    def get_hook_subclass(self: Self, type_: type[T]) -> type[T]:
        """Finds a table hook that is a subclass of the provided type"""
        if type_ in self._resolver_state().hook_subclass_cache:
            return self._resolver_state().hook_subclass_cache[type_]

        min_distance = sys.maxsize
        min_subclass = None

        for cls in type(self).__mro__:
            if issubclass(cls, TableHookResolver):
                for hook in cls._resolver_state().registered_table_hooks[::-1]:  # noqa
                    if issubclass(hook, type_):
                        # Calculate distance between superclass and child class
                        try:
                            dist = hook.__mro__.index(type_)
                            if dist < min_distance:
                                min_distance = dist
                                min_subclass = hook
                        except ValueError:
                            pass

                if min_subclass:
                    break

        if min_subclass:
            self._resolver_state().hook_subclass_cache[type_] = min_subclass
            return min_subclass

        raise RuntimeError

    def store_table(self, table: Table, task: MaterializingTask | None):
        """Stores a table in the associated transaction stage

        The store must convert the table object (`table.obj`) to the correct
        internal type. This means, that in some cases it first has to
        evaluate a lazy object. For example: if a sql based table store
        receives a sql query to store, it has to execute it first.

        The implementation details of this get handled by the registered
        TableHooks.
        """

        # In case of deferred operations, inform run context that stage
        # isn't 100% cache valid anymore.
        if task is not None:
            RunContext.get().set_stage_has_changed(task.stage)

        # Materialize
        hook = self.get_m_table_hook(type(table.obj))
        hook.materialize(self, table, table.stage.transaction_name)

    def retrieve_table_obj(
        self,
        table: Table,
        as_type: type[T],
        for_auto_versioning: bool = False,
    ) -> T:
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
        stage_name = table.stage.current_name if table.stage is not None else None
        try:
            if for_auto_versioning:
                return hook.retrieve_for_auto_versioning_lazy(
                    self, table, stage_name, as_type
                )

            return hook.retrieve(self, table, stage_name, as_type)
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve table '{table}'") from e

    __hooks_with_unmet_requirements: list[str] = []

    @classmethod
    def __hook_unmet_requirements_message(cls) -> str:
        if len(cls.__hooks_with_unmet_requirements) == 0:
            return "This is is because no TableHook has been registered for this type."

        return (
            "This is either because no TableHook has been registered for this type, "
            "or because not all requirements have been met for the corresponding hook."
            "\nHooks with unmet requirements: "
            + ", ".join(cls.__hooks_with_unmet_requirements)
        )


class BaseTableStore(TableHookResolver, Disposable):
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

    def __init__(self):
        self.logger = structlog.get_logger(logger_name=type(self).__name__)
        self.local_table_cache: BaseTableCache | None = None

    def setup(self):
        """Setup function

        This function gets called at the beginning of a flow run.
        Unlike the __init__ method, a lock is acquired before
        the setup method gets called to prevent race conditions.
        """

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

    def store_table(self, table: Table, task: MaterializingTask | None):
        super().store_table(table, task)
        if self.local_table_cache:
            self.local_table_cache.store_table(table, task)

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
        task_cache_info: TaskCacheInfo,
    ):
        """Lazily stores a table in the associated commit stage

        The same as `store_table()`, with the difference being that if the
        table object represents a lazy table / query, the store first checks
        if the same query with the same input (based on `table.cache_key`)
        has already been executed before. If yes, instead of evaluating
        the query, it just copies the previous result to the commit stage.

        Used when `lazy = True` is set for a materializing task.
        """
        config_context = ConfigContext.get()
        try:
            hook = self.get_m_table_hook(type(table.obj))
            query_str = hook.lazy_query_str(self, table.obj)
        except TypeError:
            self.logger.warning(
                f"The output table {table.name} given by a"
                f" {repr(type(table.obj))} of the lazy task {task.name} does"
                " not provide a query string. Lazy evaluation is not"
                " possible. Assuming that the table is not cache valid."
            )
            # Assign random query string to ensure that task is not cache valid
            query_str = uuid.uuid4().hex

        if table.assumed_dependencies is None:
            query_hash = stable_hash("LAZY-TABLE", query_str)
        else:
            # include assumed dependencies in query hash for imperative materialize
            dependencies = config_context.store.json_encoder.encode(
                table.assumed_dependencies
            )
            query_hash = stable_hash("LAZY-TABLE", query_str, dependencies)

        # Store the table
        try:
            if task_cache_info.force_task_execution:
                self.logger.info(
                    "Forced task execution due to config",
                    cache_validation=config_context.cache_validation,
                )
                raise CacheError("Forced task execution")
            # Try retrieving the table from the cache and then copying it
            # to the transaction stage
            metadata = self.retrieve_lazy_table_metadata(
                query_hash, task_cache_info.cache_key, table.stage
            )
            RunContext.get().trace_hook.query_cache_status(
                task,
                table,
                task_cache_info,
                query_hash,
                query_str,
                cache_metadata=metadata,
            )
            RunContext.get().trace_hook.cache_init_transfer(task, table)
            self.copy_lazy_table_to_transaction(metadata, table)
            self.logger.info(f"Lazy cache of table '{table.name}' found")
        except CacheError as e:
            # Either not found in cache, or copying failed
            # -> Store using default method
            self.logger.warning(
                "Cache miss", table=table.name, stage=table.stage.name, cause=str(e)
            )
            TaskContext.get().is_cache_valid = False
            RunContext.get().trace_hook.query_cache_status(
                task, table, task_cache_info, query_hash, query_str, cache_valid=False
            )
            if task_cache_info.assert_no_materialization:
                raise AssertionError(
                    "cache_validation.mode=ASSERT_NO_FRESH_INPUT is a "
                    "protection mechanism to prevent execution of "
                    "source tasks to keep pipeline input stable. However,"
                    "this table was still about to be materialized: "
                    f"{table.stage.name}.{table.name}"
                ) from None
            self.store_table(table, task)
            RunContext.get().trace_hook.query_complete(task, table)

        # Store table metadata
        self.store_lazy_table_metadata(
            LazyTableMetadata(
                name=table.name,
                stage=table.stage.name,
                query_hash=query_hash,
                task_hash=task_cache_info.cache_key,
            )
        )

        # At this point we MUST also update the cache info, so that any downstream
        # tasks get invalidated if the sql query string changed.
        table.cache_key = lazy_table_cache_key(task_cache_info.cache_key, query_hash)

    def store_raw_sql(
        self, raw_sql: RawSql, task: MaterializingTask, task_cache_info: TaskCacheInfo
    ):
        """Lazily stores a table in the associated commit stage

        The same as `store_table()`, with the difference being that the store first
        checks if the same query with the same input (based on `raw_sql.cache_key`)
        has already been executed before. If yes, instead of evaluating
        the query, it just copies the previous result to the commit stage.
        """

        # hacky way to canonicalize query (despite __tmp/__even/__odd suffixes
        # and alias resolution)
        import re

        query_str = raw_sql.sql
        query_str = re.sub(r'["\[\]]', "", query_str)
        query_str = re.sub(
            r'(__tmp|__even|__odd)(?=[ \t\n.;"]|$)', "", query_str.lower()
        )

        query_hash = stable_hash("RAW-SQL", query_str)

        # Store raw sql
        try:
            if task_cache_info.force_task_execution:
                config_context = ConfigContext.get()
                self.logger.info(
                    "Forced task execution due to config",
                    cache_validation=config_context.cache_validation,
                )
                raise CacheError("Forced task execution")
            # Try retrieving the table from the cache and then copying it
            # to the transaction stage
            metadata = self.retrieve_raw_sql_metadata(
                query_hash, task_cache_info.cache_key, raw_sql.stage
            )
            self.copy_raw_sql_tables_to_transaction(metadata, raw_sql.stage)
            self.logger.info(f"Lazy cache of stage '{raw_sql.stage}' found")

            prev_objects = metadata.prev_objects
            new_objects = metadata.new_objects
        except CacheError as e:
            # Either not found in cache, or copying failed
            # -> Store using default method
            self.logger.warning("Cache miss for raw-SQL", cause=str(e))

            TaskContext.get().is_cache_valid = False
            RunContext.get().set_stage_has_changed(task.stage)

            if task_cache_info.assert_no_materialization:
                raise AssertionError(
                    "cache_validation.mode=ASSERT_NO_FRESH_INPUT is a "
                    "protection mechanism to prevent execution of "
                    "source tasks to keep pipeline input stable. However,"
                    f"this raw SQL script was still about to be executed: {raw_sql}"
                ) from None

            prev_objects = self.get_objects_in_stage(raw_sql.stage)
            self.execute_raw_sql(raw_sql)
            post_objects = self.get_objects_in_stage(raw_sql.stage)

            # Object names must be sorted to ensure that we can identify the task
            # again in the future even if the objects get returned in a different order.
            prev_objects = sorted(prev_objects)

            prev_objects_set = set(prev_objects)
            new_objects = [o for o in post_objects if o not in prev_objects_set]

        # Store metadata
        # Attention: Raw SQL statements may only be executed sequentially within
        #            stage for store.get_objects_in_stage to work
        self.store_raw_sql_metadata(
            RawSqlMetadata(
                prev_objects=prev_objects,
                new_objects=new_objects,
                stage=raw_sql.stage.name,
                query_hash=query_hash,
                task_hash=task_cache_info.cache_key,
            )
        )

        # At this point we MUST also update the cache info, so that any downstream
        # tasks get invalidated if the sql query string changed.
        raw_sql.cache_key = lazy_table_cache_key(task_cache_info.cache_key, query_hash)

        # Store new_objects as part of raw_sql.
        all_table_names = set(self.get_table_objects_in_stage(raw_sql.stage))
        raw_sql.table_names = sorted(o for o in new_objects if o in all_table_names)

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

    def retrieve_table_obj(
        self,
        table: Table,
        as_type: type[T] | None,
        for_auto_versioning: bool = False,
    ) -> T:
        if as_type is None:
            # Simply return enough information that a user could dematerialize the table
            # or perform it with some other library.
            # hint: `schema = table_store.get_schema(table.stage.current_name).get()`
            return table.name, table.stage.current_name

        if for_auto_versioning:
            return super().retrieve_table_obj(table, as_type, for_auto_versioning)

        if self.local_table_cache:
            obj = self.local_table_cache.retrieve_table_obj(table, as_type)
            if obj is not None:
                return obj

        obj = super().retrieve_table_obj(table, as_type)

        if self.local_table_cache:
            t = table.copy_without_obj()
            t.obj = obj
            self.local_table_cache.store_input(t, task=None)

        return obj

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

    @abstractmethod
    def retrieve_all_task_metadata(
        self, task: MaterializingTask, ignore_position_hashes: bool = False
    ) -> list[TaskMetadata]:
        """Retrieves all metadata objects associated with a task from the store

        As long as a metadata entry has the same task and stage name, as well
        as the same position hash as the `task` object, it should get returned.
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
    def get_objects_in_stage(self, stage: Stage) -> list[str]:
        """
        List all objects that are in the current stage.

        This may include tables but also other database objects like views, stored
        procedures, functions etc. This function is used to calculate a diff on the
        table store to determine which objects were produced (or could have been used
        to produce those objects) when executing RawSQL.

        :param stage: the stage
        :return: list of object names in the stage at the current point in time.
        """

    @abstractmethod
    def get_table_objects_in_stage(self, stage: Stage, include_views=True) -> list[str]:
        """
        List all table-like objects that are in the current stage.

        :param stage: the stage
        :return: list of table-like object names in the stage at
            the current point in time.
        """


class AutoVersionSupport(Enum):
    NONE = 0
    LAZY = 1
    TRACE = 2


class TableHook(Generic[TableHookResolverT], ABC):
    """Base class to define how to handle a specific table

    For more information, take a look at the `BaseTableStore.register_table`
    documentation.
    """

    __slots__ = ()
    auto_version_support: AutoVersionSupport = AutoVersionSupport.NONE

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
    def retrieve_as_reference(cls, type_: type) -> bool:
        """
        Table hooks that retrieve `type_` as reference instead of copying the content
        of the table should return True. This is used by imperative materialization
        to determine whether input_type is a good default for returned references.
        """
        return False

    @classmethod
    @abstractmethod
    def materialize(
        cls, store: TableHookResolverT, table: Table, stage_name: str
    ) -> None:
        """Materialize a table object

        :param store: The store which called this method
        :param table: The table that should be materialized
        :param stage_name: The name of the stage in which the table should
            be stored - can either be `stage.name` or `stage.transaction_name`.
        """

    @classmethod
    @abstractmethod
    def retrieve(
        cls,
        store: TableHookResolverT,
        table: Table,
        stage_name: str | None,
        as_type: type[T] | tuple | dict[str, Any],
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
    def lazy_query_str(cls, store: TableHookResolverT, obj) -> str:
        """String that represents the associated object

        Can either be a literal query string (e.g. SQL query), or a string that
        uniquely represents the query in some other way. Used for computing
        the cache key for materializing tasks with `lazy=True`.

        :raises TypeError: if the type doesn't support lazy queries.
        """
        raise TypeError(f"Lazy query not supported with object of type {type(obj)}")

    @classmethod
    def retrieve_for_auto_versioning_lazy(
        cls,
        store: TableHookResolverT,
        table: Table,
        stage_name: str,
        as_type: type[T] | tuple | dict[str, Any],
    ) -> T:
        """
        Retrieve a table (or table like object) from the store to be used for
        automatic version number determination.

        This function gets called with the same arguments as ``.retrieve``.

        Must be implemented for ``auto_version_support == AutoVersionSupport.LAZY``.

        :raises TypeError: if the type doesn't support automatic versioning.
        """
        raise TypeError(f"Auto versioning not supported for type {as_type}")

    @classmethod
    def get_auto_version_lazy(cls, obj) -> str:
        """
        Must be implemented for ``auto_version_support == AutoVersionSupport.LAZY``.

        :param obj: object returned from task
        :return: string representation of the operations performed on this object.
        :raises TypeError: if the object doesn't support automatic versioning.
        """
        raise NotImplementedError

    @classmethod
    def get_computation_tracer(cls) -> ComputationTracer:
        """
        Get a computation tracer for this type of task.
        Must be implemented for ``auto_version_support == AutoVersionSupport.TRACE``.
        """
        raise NotImplementedError
