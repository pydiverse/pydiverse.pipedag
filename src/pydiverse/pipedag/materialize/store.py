# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable

import structlog

from pydiverse.common.util import Disposable, deep_map
from pydiverse.common.util.hashing import stable_hash
from pydiverse.pipedag import Blob, Schema, Stage, Table
from pydiverse.pipedag._typing import Materializable, T
from pydiverse.pipedag.container import RawSql, attach_annotation
from pydiverse.pipedag.context import ConfigContext, RunContext, TaskContext
from pydiverse.pipedag.context.run_context import StageState
from pydiverse.pipedag.core.config import PipedagConfig
from pydiverse.pipedag.core.task import Task, TaskGetItem
from pydiverse.pipedag.errors import (
    CacheError,
    DuplicateNameError,
    HookCheckException,
    StageError,
    StoreIncompatibleException,
)
from pydiverse.pipedag.materialize.cache import TaskCacheInfo, lazy_table_cache_key
from pydiverse.pipedag.materialize.materializing_task import MaterializingTask
from pydiverse.pipedag.materialize.metadata import (
    LazyTableMetadata,
    RawSqlMetadata,
    TaskMetadata,
)
from pydiverse.pipedag.materialize.table_hook_base import TableHookResolver


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
        if self.local_table_cache:
            self.local_table_cache.store_table(table, task)
        super().store_table(table, task)

    def execute_raw_sql(self, raw_sql: RawSql):
        """Executed raw SQL statements in the associated transaction stage

        This method is overridden by actual table stores that can handle raw SQL.
        """

        raise NotImplementedError("This table store does not support executing raw sql statements")

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
            hook = self.get_m_table_hook(table)
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
            dependencies = config_context.store.json_encoder.encode(table.assumed_dependencies)
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
            metadata = self.retrieve_lazy_table_metadata(query_hash, task_cache_info.cache_key, table.stage)
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
            self.logger.warning("Cache miss", table=table.name, stage=table.stage.name, cause=str(e))
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

    def store_raw_sql(self, raw_sql: RawSql, task: MaterializingTask, task_cache_info: TaskCacheInfo):
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
        query_str = re.sub(r'(__tmp|__even|__odd)(?=[ \t\n.;"]|$)', "", query_str.lower())

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
            metadata = self.retrieve_raw_sql_metadata(query_hash, task_cache_info.cache_key, raw_sql.stage)
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

    def copy_raw_sql_tables_to_transaction(self, metadata: RawSqlMetadata, stage: Stage):
        """Copy all tables identified by the metadata as generated by raw
        SQL statements to the transaction stage.

        This operation MUST not remove the table from the base stage or modify
        it in any way.

        :raises CacheError: if the lazy table can't be found
        """
        raise NotImplementedError("This table store does not support executing raw sql statements")

    @abstractmethod
    def delete_table_from_transaction(self, table: Table, *, schema: Schema | None = None):
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
    def retrieve_task_metadata(self, task: MaterializingTask, input_hash: str, cache_fn_hash: str) -> TaskMetadata:
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
    def retrieve_lazy_table_metadata(self, query_hash: str, task_hash: str, stage: Stage) -> LazyTableMetadata:
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
        raise NotImplementedError("This table store does not support executing raw sql statements")

    def retrieve_raw_sql_metadata(self, query_hash: str, task_hash: str, stage: Stage) -> RawSqlMetadata:
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
        raise NotImplementedError("This table store does not support executing raw sql statements")

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


class BaseTableCache(ABC, TableHookResolver, Disposable):
    def __init__(
        self,
        store_input: bool = True,
        store_output: bool = False,
        use_stored_input_as_cache: bool = True,
    ):
        super().__init__()

        self.logger = structlog.get_logger(logger_name=type(self).__name__)

        self.should_store_input = store_input
        self.should_store_output = store_output
        self.should_use_stored_input_as_cache = use_stored_input_as_cache

    def setup(self):
        """Setup function

        This function gets called at the beginning of a flow run.
        Unlike the __init__ method, a lock is acquired before
        the setup method gets called to prevent race conditions.
        """

    def init_stage(self, stage: Stage):
        """Initialize a stage

        Gets called before any table is attempted to be stored in the stage.
        """

    @abstractmethod
    def clear_cache(self, stage: Stage):
        """Delete the cache for a specific stage"""

    def store_table(self, table: Table, task: MaterializingTask):
        if self.should_store_output:
            return self._store_table(table, task)

    def store_input(self, table: Table, task: MaterializingTask):
        if self.should_store_input:
            return self._store_table(table, task)

    def _store_table(self, table: Table, task: MaterializingTask | None) -> bool:
        """
        :return: bool flag indicating if storing was successful
        """
        try:
            hook = self.get_m_table_hook(table)
        except TypeError:
            return False

        if not RunContext.get().should_store_table_in_cache(table):
            # Prevent multiple tasks writing at the same time
            return False

        try:
            hook.materialize(self, table, table.stage.transaction_name)
        except TypeError:
            return False
        return True

    def retrieve_table_obj(
        self,
        table: Table,
        as_type: type[T],
        for_auto_versioning: bool = False,
    ) -> T:
        assert not for_auto_versioning

        if not self.should_use_stored_input_as_cache:
            return None
        if not self._has_table(table, as_type):
            return None
        return self._retrieve_table_obj(table, as_type)

    def _retrieve_table_obj(self, table: Table, as_type: type[T]) -> T:
        try:
            hook = self.get_r_table_hook(as_type)
            obj = hook.retrieve(self, table, table.stage.name, as_type)
            self.logger.info("Retrieved table from local table cache", table=table)
            return obj
        except StoreIncompatibleException:
            # This is expected for example when ParquetTableCache is asked to retrieve a SQL reference
            return None
        except Exception as e:
            self.logger.warning(
                "Failed to retrieve table from local table cache",
                table=table,
                cause=str(e),
            )
            return None

    @abstractmethod
    def _has_table(self, table: Table, as_type: type) -> bool:
        """Check if the given table is in the cache"""


class BaseBlobStore(Disposable, ABC):
    """Blob store base class

    A blob (binary large object) store is responsible for storing arbitrary
    python objects. This can, for example, be done by serializing them using
    the python ``pickle`` module.

    A store must use a blob's name (``Blob.name``) and stage (``Blob.stage``)
    as the primary keys for storing and retrieving blobs. This means that
    two different ``Blob`` objects can be used to store and retrieve the same
    data as long as they have the same name and stage.
    """

    @abstractmethod
    def init_stage(self, stage: Stage):
        """Initialize a stage and start a transaction"""

    @abstractmethod
    def commit_stage(self, stage: Stage):
        """Commit the stage transaction

        Replace the blobs of the base stage with the blobs in the transaction.
        """

    @abstractmethod
    def store_blob(self, blob: Blob):
        """Stores a blob in the associated stage transaction"""

    @abstractmethod
    def copy_blob_to_transaction(self, blob: Blob):
        """Copy a blob from the base stage to the transaction

        This operation MUST not remove the blob from the base stage or modify
        it in any way.
        """

    @abstractmethod
    def delete_blob_from_transaction(self, blob: Blob):
        """Delete a blob from the transaction

        If the blob doesn't exist in the transaction, fail silently.
        """

    @abstractmethod
    def retrieve_blob(self, blob: Blob) -> Any:
        """Loads a blob from the store

        Retrieves the stored python object from the store and returns it.
        If the stage hasn't yet been committed, the blob must be retrieved
        from the transaction, else it must be retrieved from the committed
        stage.
        """


class PipeDAGStore(Disposable):
    """Main storage interface for materializing tasks

    Depending on the use case, the store can be configured using different
    backends for storing tables, blobs and managing locks.

    Other than initializing the global `PipeDAGStore` object, the user
    should never have to interact with it. It only serves as a coordinator
    between the different backends, the stages and the materializing tasks.
    """

    def __init__(
        self,
        table: BaseTableStore,
        blob: BaseBlobStore,
        local_table_cache: BaseTableCache | None,
    ):
        self.table_store = table
        self.blob_store = blob
        self.local_table_cache = local_table_cache
        self.table_store.local_table_cache = local_table_cache

        self.logger = structlog.get_logger()

        from pydiverse.pipedag.util.json import PipedagJSONDecoder, PipedagJSONEncoder

        self.json_encoder = PipedagJSONEncoder()
        self.json_decoder = PipedagJSONDecoder()

    def dispose(self):
        """
        Clean up and close all open resources.

        Don't use the store object any more after disposal!
        """
        self.table_store.dispose()
        self.blob_store.dispose()
        if self.local_table_cache:
            self.local_table_cache.dispose()
        super().dispose()

    # ### Stage ### #

    def init_stage(self, stage: Stage):
        """Initializes the stage in all backends

        This function also acquires a lock on the given stage to prevent
        other flows from modifying the same stage at the same time. Only
        once all tasks that depend on this stage have been executed, will
        this lock be released.

        Don't use this function directly. Instead, use `ensure_stage_is_ready`
        to prevent unnecessary locking.
        """

        RunContext.get().validate_stage_lock(stage)
        self.table_store.init_stage(stage)
        self.blob_store.init_stage(stage)
        if self.local_table_cache:
            self.local_table_cache.init_stage(stage)

        # Once stage reference counter hits 0 (this means all tasks that
        # have any task contained in the stage as an upstream dependency),
        # the lock gets automatically released by the RunContextServer

    def ensure_stage_is_ready(self, stage: Stage):
        """Initializes a stage if it hasn't been created yet

        This allows the creation of stages in a lazy way. This ensures
        that a stage only gets locked right before it is needed.
        """
        with RunContext.get().init_stage(stage) as should_continue:
            if not should_continue:
                return
            self.init_stage(stage)

    def commit_stage(self, stage: Stage):
        """Commit the stage"""
        ctx = RunContext.get()
        with ctx.commit_stage(stage) as should_continue:
            if not should_continue:
                raise StageError
            ctx.validate_stage_lock(stage)
            self.table_store.commit_stage(stage)
            self.blob_store.commit_stage(stage)

    # ### Materialization ### #

    def dematerialize_item(
        self,
        item: Table | RawSql | Blob | Any,
        as_type: type[T] | None,
        ctx: RunContext | None = None,
        for_auto_versioning: bool = False,
    ):
        if ctx is None:
            ctx = RunContext.get()

        if isinstance(item, Table):
            # item.stage can be None for ExternalTableReference
            if item.stage is not None:
                ctx.validate_stage_lock(item.stage)
            obj = self.table_store.retrieve_table_obj(
                item,
                as_type=as_type,
                for_auto_versioning=for_auto_versioning,
            )
            return obj
        elif isinstance(item, RawSql):
            ctx.validate_stage_lock(item.stage)

            loaded_tables = {}
            for table_name in item:
                table = item[table_name]
                obj = self.table_store.retrieve_table_obj(
                    table,
                    as_type=as_type,
                    for_auto_versioning=for_auto_versioning,
                )
                loaded_tables[table_name] = obj

            new_raw_sql = item.copy_without_obj()
            new_raw_sql.loaded_tables = loaded_tables
            return new_raw_sql
        elif isinstance(item, Blob):
            ctx.validate_stage_lock(item.stage)
            return self.blob_store.retrieve_blob(item)
        return item

    def dematerialize_task_inputs(
        self,
        task: MaterializingTask,
        args: tuple[Materializable],
        kwargs: dict[str, Materializable],
        *,
        for_auto_versioning: bool = False,
    ) -> tuple[tuple, dict]:
        """Loads the inputs for a task from the storage backends

        Traverses the function arguments and replaces all `Table` and
        `Blob` objects with the associated objects stored in the backend.

        :param task: The task for which the arguments should be dematerialized
        :param args: The positional arguments
        :param kwargs: The keyword arguments
        :param for_auto_versioning: If the task inputs should be retrieved
            for use with auto versioning.
        :return: A tuple with the dematerialized args and kwargs
        """

        ctx = RunContext.get()

        def dematerialize_mapper(x):
            ret = self.dematerialize_item(
                x,
                as_type=task.input_type,
                ctx=ctx,
                for_auto_versioning=for_auto_versioning,
            )
            if task.add_input_source and isinstance(x, (Table, Blob, RawSql)):
                return ret, x
            return ret

        d_args = deep_map(args, dematerialize_mapper)
        d_kwargs = deep_map(kwargs, dematerialize_mapper)

        return d_args, d_kwargs

    def materialize_task(
        self,
        task: MaterializingTask,
        task_cache_info: TaskCacheInfo,
        value: Materializable,
        disable_task_finalization=False,
    ) -> Materializable:
        """Stores the output of a task in the backend

        Traverses the output produced by a task, adds missing metadata,
        materializes all `Table` and `Blob` objects and returns a new
        output object with the required metadata to allow dematerialization.

        :param task: The task instance which produced `value`. Must have
            the correct `cache_key` attribute set.
        :param task_cache_info: Task cache information.
        :param value: The output of the task. Must be materializable; this
            means it can only contain the following object types:
            `dict`, `list`, `tuple`,
            `int`, `float`, `str`, `bool`, `None`,
            and PipeDAG's `Table` and `Blob` type.
        :param disable_task_finalization: True for imperative materialization
            when task is not finished, yet.
        :return: A copy of `value` with additional metadata
        """

        stage = task.stage
        ctx = RunContext.get()

        if (state := ctx.get_stage_state(stage)) != StageState.READY:
            raise StageError(f"Can't materialize because stage '{stage.name}' is not ready (state: {state}).")

        value, tables, raw_sqls, blobs = self.prepare_task_output_for_materialization(task, task_cache_info, value)

        def store_metadata():
            """
            Metadata must be generated after everything else has been materialized,
            because during materialization the cache_key of the different objects
            can get changed.
            """
            if not disable_task_finalization:
                output_json = self.json_encode(value)
                metadata = TaskMetadata(
                    name=task.name,
                    stage=task.stage.name,
                    version=task.version,
                    timestamp=datetime.now(),
                    run_id=ctx.run_id,
                    position_hash=task.position_hash,
                    input_hash=task_cache_info.input_hash,
                    cache_fn_hash=task_cache_info.cache_fn_hash,
                    output_json=output_json,
                )
                self.table_store.store_task_metadata(metadata, stage)

        def store_table(table: Table):
            if task.lazy:
                self.table_store.store_table_lazy(table, task, task_cache_info)
            else:
                self.table_store.store_table(table, task)

        # Materialize
        self._check_names(task, tables, blobs)
        check_failures = self._store_task_transaction(
            task,
            tables,
            raw_sqls,
            blobs,
            store_table,
            lambda raw_sql: self.table_store.store_raw_sql(raw_sql, task, task_cache_info),
            self.blob_store.store_blob,
            store_metadata,
        )
        if len(check_failures) > 0:
            self.logger.error(
                f"{len(check_failures)} output validation checks failed",
                failures=check_failures,
            )
            raise check_failures[0][1]

        return value

    def prepare_task_output_for_materialization(
        self,
        task: MaterializingTask,
        task_cache_info: TaskCacheInfo,
        value: Materializable,
    ) -> tuple[Materializable, list[Table], list[RawSql], list[Blob]]:
        tables = []
        raw_sqls = []
        blobs = []

        config = ConfigContext.get()
        auto_suffix_counter = task_cache_info.imperative_materialization_state.auto_suffix_counter

        def preparation_mutator(x):
            # Automatically convert an object to a table / blob if its
            # type is inside either `config.auto_table` or `.auto_blob`.
            if isinstance(x, config.auto_table):
                try:
                    hook = self.table_store.get_m_table_hook(Table(x))
                    x = hook.auto_table(x)
                except TypeError:
                    x = Table(x)
            if isinstance(x, config.auto_blob):
                x = Blob(x)

            if isinstance(x, PipedagConfig):
                # Config objects are not an allowed return type,
                # because they might mess up caching.
                raise TypeError("You can't return a PipedagConfig object from a materializing task.")
            if isinstance(x, ConfigContext):
                # Config objects are not an allowed return type,
                # because they might mess up caching.
                raise TypeError("You can't return a ConfigContext object from a materializing task.")

            # Add missing metadata (if table was not already imperatively materialized)
            if (
                isinstance(x, (Table, RawSql, Blob))
                and id(x) not in task_cache_info.imperative_materialization_state.table_ids
            ):
                if not task.lazy:
                    # task cache_key is output cache_key for eager tables
                    x.cache_key = task_cache_info.cache_key

                x.stage = task.stage

                # Update name:
                # - If no name has been provided, generate on automatically
                # - If the provided name ends with %%, perform name mangling
                object_number = next(auto_suffix_counter)
                auto_suffix = f"{task_cache_info.cache_key}_{object_number:04d}"

                x.name = mangle_table_name(x.name, task.name, auto_suffix)

                if isinstance(x, Table):
                    if x.obj is None:
                        raise TypeError("Underlying table object can't be None")
                    tables.append(x)
                elif isinstance(x, RawSql):
                    if x.sql is None:
                        raise TypeError("Underlying raw sql string can't be None")
                    raw_sqls.append(x)
                elif isinstance(x, Blob):
                    if task.lazy:
                        raise NotImplementedError(
                            "Can't use Blobs with lazy tasks. Invalidation of the"
                            " downstream dependencies is not implemented."
                        )
                    blobs.append(x)
                else:
                    raise NotImplementedError

            return x

        value = deep_map(value, preparation_mutator)
        attach_annotation(task.fn_annotations.get("return"), value)

        return value, tables, raw_sqls, blobs

    @staticmethod
    def _check_names(task: MaterializingTask, tables: list[Table], blobs: list[Blob]):
        if not tables and not blobs:
            # Nothing to check
            return

        # Check names: No duplicates in task
        seen_tn = set()
        seen_bn = set()
        tn_dup = [e.name for e in tables if e.name in seen_tn or seen_tn.add(e.name)]
        bn_dup = [e.name for e in blobs if e.name in seen_bn or seen_bn.add(e.name)]

        if tn_dup or bn_dup:
            raise DuplicateNameError(
                f"Task '{task.name}' returned multiple tables and/or blobs"
                " with the same name.\n"
                f"Duplicate table names: {', '.join(tn_dup) if tn_dup else 'None'}\n"
                f"Duplicate blob names: {', '.join(bn_dup) if bn_dup else 'None'}\n"
                "To enable automatic name mangling,"
                " you can add '%%' at the end of the name."
            )

        # Check names: No duplicates in stage
        ctx = RunContext.get()
        success, tn_dup, bn_dup = ctx.add_names(task.stage, tables, blobs)

        if not success:
            raise DuplicateNameError(
                f"Task '{task.name}' returned tables and/or blobs"
                f" whose name are not unique in the schema '{task.stage}'.\n"
                f"Duplicate table names: {', '.join(tn_dup) if tn_dup else 'None'}\n"
                f"Duplicate blob names: {', '.join(bn_dup) if bn_dup else 'None'}\n"
                "To enable automatic name mangling,"
                " you can add '%%' at the end of the name."
            )

    def _store_task_transaction(
        self,
        task: MaterializingTask,
        tables: list[Table],
        raw_sqls: list[RawSql],
        blobs: list[Blob],
        store_table: Callable[[Table], None],
        store_raw_sql: Callable[[RawSql], None],
        store_blob: Callable[[Blob], None],
        store_metadata: Callable[[], None],
    ) -> list[tuple[Table, Exception]]:
        stage = task.stage
        ctx = RunContext.get()

        stored_tables = []
        stored_blobs = []
        check_failures = []

        try:
            for table in tables:
                ctx.validate_stage_lock(stage)
                try:
                    store_table(table)
                except HookCheckException as e:
                    check_failures.append((table, e))

                stored_tables.append(table)
            for blob in blobs:
                ctx.validate_stage_lock(stage)
                store_blob(blob)
                stored_blobs.append(blob)
            for raw_sql in raw_sqls:
                ctx.validate_stage_lock(stage)
                store_raw_sql(raw_sql)

            ctx.validate_stage_lock(task.stage)
            store_metadata()

        except Exception as e:
            if ConfigContext.get().fail_fast:
                raise e

            # Failed - Roll back everything
            for table in stored_tables:
                self.table_store.delete_table_from_transaction(table)
            for blob in stored_blobs:
                self.blob_store.delete_blob_from_transaction(blob)

            ctx.remove_names(stage, tables, blobs)
            raise e

        return check_failures

    # ### Cache ### #

    def retrieve_cached_output(
        self,
        task: MaterializingTask,
        input_hash: str,
        cache_fn_hash: str,
    ) -> (Materializable, TaskMetadata):
        """Try to retrieve the cached outputs for a task

        :param task: The materializing task for which to retrieve
            the cached output. Must have the `cache_key` attribute set.
        :raises CacheError: if no matching task exists in the cache
        """

        if task.stage.did_commit:
            raise StageError(f"Stage ({task.stage}) already committed.")

        metadata = self.table_store.retrieve_task_metadata(task, input_hash, cache_fn_hash)
        return self.json_decode(metadata.output_json), metadata

    def copy_cached_output_to_transaction_stage(
        self,
        output: Materializable,
        original_metadata: TaskMetadata,
        task: MaterializingTask,
    ):
        """Copy the (non-lazy) outputs from a cached task into the transaction stage

        If the outputs of a task were successfully retrieved from the cache
        using `retrieve_cached_output`, they and the associated metadata
        must be copied from the base stage to the transaction.

        :raises CacheError: if some values in the output can't be found
            in the cache.
        """

        # Get Tables and Blobs from output
        tables = []
        blobs = []
        raw_sqls = []

        def visitor(x):
            if isinstance(x, Table):
                # Tables in external schemas should not get copied
                if x.external_schema is None:
                    tables.append(x)
            elif isinstance(x, RawSql):
                raw_sqls.append(x)
            elif isinstance(x, Blob):
                blobs.append(x)
            return x

        deep_map(output, visitor)

        def store_raw_sql(raw_sql):
            raise Exception("raw sql scripts cannot be part of a non-lazy task")

        # Materialize
        self._check_names(task, tables, blobs)
        self._store_task_transaction(
            task,
            tables,
            raw_sqls,
            blobs,
            self.table_store.copy_table_to_transaction,
            store_raw_sql,
            self.blob_store.copy_blob_to_transaction,
            lambda: self.table_store.store_task_metadata(original_metadata, task.stage),
        )

    def retrieve_most_recent_task_output_from_cache(
        self, task: MaterializingTask, ignore_position_hashes: bool = False
    ) -> (Materializable, TaskMetadata):
        """
        Retrieves the cached output from the most recent execution of a task.

        This retrieval is done based on the name, stage and position hash
        of the task.

        :param task: The materializing task for which to retrieve
            the cached output.
        :param ignore_position_hashes:
            If ``True``, the position hashes of tasks are not checked
            when retrieving the inputs of a task from the cache.
            This simplifies execution of subgraphs if you don't care whether inputs to
            that subgraph are cache invalid. This allows multiple modifications in the
            Graph before the next run updating the cache.
            Attention: This may break automatic cache invalidation.
            And for this to work, any task producing an input
            for the chosen subgraph may never be used more
            than once per stage.
        :return: The output from the task as well as the corresponding metadata.
        :raises CacheError: if no matching task exists in the cache
        """

        # This returns all metadata objects with the same name, stage, AND position
        # hash as `task`. We utilize the position hash to identify a specific
        # task instance, if the same task appears multiple times in a stage.
        metadata = self.table_store.retrieve_all_task_metadata(task, ignore_position_hashes=ignore_position_hashes)

        if not metadata:
            raise CacheError(f"Couldn't find cached output for task '{task}' with matching position hash.")

        # Choose newest entry
        newest_metadata = sorted(metadata, key=lambda m: m.timestamp)[-1]
        cached_output = self.json_decode(newest_metadata.output_json)

        return cached_output, newest_metadata

    # ### Utils ### #

    def json_encode(self, value: Materializable) -> str:
        """Encode a materializable value as json

        In addition to the default types that python can serialise to json,
        this function can also serialise `Table`, `RawSql`, and `Blob` objects.
        The only caveat is, that python's json module doesn't differentiate
        between lists and tuples, which means it is impossible to
        differentiate the two.
        """
        return self.json_encoder.encode(value)

    def json_decode(self, value: str) -> Materializable:
        """Decode a materializable value as json

        Counterpart for the `json_encode` function. Can decode `Table` and
        `Blob` objects.
        """
        return self.json_decoder.decode(value)


def dematerialize_output_from_store(
    store: PipeDAGStore,
    task: Task | TaskGetItem,
    task_output: Materializable,
    as_type: type | None,
) -> Any:
    """
    Dematerializes a task's output from the store

    Must be called inside a ConfigContext and RunContext. Instead
    of a RunContext, a DematerializeRunContext can also be used.
    """
    if isinstance(task, Task):
        root_task = task
    else:
        root_task = task.task

    if as_type is None:
        assert isinstance(root_task, MaterializingTask)
        as_type = root_task.input_type

    run_context = RunContext.get()
    task_output = task.resolve_value(task_output)

    with TaskContext(task):
        return deep_map(
            task_output,
            lambda item: store.dematerialize_item(item, as_type=as_type, ctx=run_context),
        )


def mangle_table_name(table_name: str, task_name: str | None, suffix: str):
    if table_name is None:
        if task_name is None:
            return suffix
        table_name = task_name + "_" + suffix
    elif table_name.endswith("%%"):
        table_name = table_name[:-2] + suffix
    return table_name
