from __future__ import annotations

import itertools
from typing import Any, Callable

import structlog

from pydiverse.pipedag import Blob, Stage, Table, backend
from pydiverse.pipedag._typing import Materializable, T
from pydiverse.pipedag.context import ConfigContext, RunContext, TaskContext
from pydiverse.pipedag.context.run_context import StageState
from pydiverse.pipedag.core.config import PipedagConfig
from pydiverse.pipedag.core.task import Task, TaskGetItem
from pydiverse.pipedag.errors import CacheError, DuplicateNameError, StageError
from pydiverse.pipedag.materialize.container import RawSql
from pydiverse.pipedag.materialize.core import MaterializingTask, TaskInfo
from pydiverse.pipedag.materialize.metadata import TaskMetadata
from pydiverse.pipedag.util import Disposable, deep_map


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
        table: backend.table.BaseTableStore,
        blob: backend.blob.BaseBlobStore,
        local_table_cache: backend.table.cache.BaseTableCache | None,
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
        as_type: type[T],
        ctx: RunContext | None = None,
    ):
        if ctx is None:
            ctx = RunContext.get()

        if isinstance(item, Table):
            ctx.validate_stage_lock(item.stage)
            obj = self.table_store.retrieve_table_obj(item, as_type=as_type)
            return obj
        elif isinstance(item, RawSql):
            ctx.validate_stage_lock(item.stage)

            loaded_tables = {}
            for table_name in item:
                table = item[table_name]
                obj = self.table_store.retrieve_table_obj(table, as_type=as_type)
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
    ) -> tuple[tuple, dict, list[Table]]:
        """Loads the inputs for a task from the storage backends

        Traverses the function arguments and replaces all `Table` and
        `Blob` objects with the associated objects stored in the backend.

        :param task: The task for which the arguments should be dematerialized
        :param args: The positional arguments
        :param kwargs: The keyword arguments
        :return: A tuple with the dematerialized args and kwargs
        """

        ctx = RunContext.get()

        input_tables = []

        def dematerialize_mapper(x):
            if isinstance(x, Table):
                input_tables.append(x)
            return self.dematerialize_item(x, as_type=task.input_type, ctx=ctx)

        d_args = deep_map(args, dematerialize_mapper)
        d_kwargs = deep_map(kwargs, dematerialize_mapper)

        return d_args, d_kwargs, input_tables

    def materialize_task(
        self,
        task: MaterializingTask,
        task_info: TaskInfo,
        value: Materializable,
    ) -> Materializable:
        """Stores the output of a task in the backend

        Traverses the output produced by a task, adds missing metadata,
        materializes all `Table` and `Blob` objects and returns a new
        output object with the required metadata to allow dematerialization.

        :param task: The task instance which produced `value`. Must have
            the correct `cache_key` attribute set.
        :param task_info: Information about task carried through materialization
        :param value: The output of the task. Must be materializable; this
            means it can only contain the following object types:
            `dict`, `list`, `tuple`,
            `int`, `float`, `str`, `bool`, `None`,
            and PipeDAG's `Table` and `Blob` type.
        :return: A copy of `value` with additional metadata
        """

        stage = task.stage
        ctx = RunContext.get()

        if (state := ctx.get_stage_state(stage)) != StageState.READY:
            raise StageError(
                f"Can't materialize because stage '{stage.name}' is not ready "
                f"(state: {state})."
            )

        tables = []
        raw_sqls = []
        blobs = []

        config = ConfigContext.get()
        auto_suffix_counter = itertools.count()

        def materialize_mutator(x):
            # Automatically convert an object to a table / blob if its
            # type is inside either `config.auto_table` or `.auto_blob`.
            if isinstance(x, config.auto_table):
                try:
                    hook = self.table_store.get_m_table_hook(type(x))
                    x = hook.auto_table(x)
                except TypeError:
                    x = Table(x)
            if isinstance(x, config.auto_blob):
                x = Blob(x)

            if isinstance(x, PipedagConfig):
                # Config objects are not an allowed return type,
                # because they might mess up caching.
                raise TypeError(
                    "You can't return a PipedagConfig object from a materializing task."
                )

            # Do the materialization
            if isinstance(x, (Table, RawSql, Blob)):
                if not task.lazy:
                    # task cache_key is output cache_key for eager tables
                    x.cache_key = task_info.task_cache_info.get_task_cache_key()

                x.stage = stage

                # Update name:
                # - If no name has been provided, generate on automatically
                # - If the provided name ends with %%, perform name mangling
                object_number = next(auto_suffix_counter)
                auto_suffix = (
                    f"{task_info.task_cache_info.get_task_cache_key()}"
                    f"_{object_number:04d}"
                )

                if x.name is None:
                    x.name = task.name + "_" + auto_suffix
                elif x.name.endswith("%%"):
                    x.name = x.name[:-2] + auto_suffix

                ctx.validate_stage_lock(stage)
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

        m_value = deep_map(value, materialize_mutator)

        def store_metadata():
            """
            Metadata must be generated after everything else has been materialized,
            because during materialization the cache_key of the different objects
            can get changed.
            """
            output_json = self.json_encode(m_value)
            task_info.task_cache_info.store_task_metadata(
                output_json, self.table_store, stage
            )

        def store_table(table: Table):
            if task.lazy:
                self.table_store.store_table_lazy(table, task, task_info)
            else:
                self.table_store.store_table(table, task, task_info)

        # Materialize
        self._check_names(task, tables, blobs)
        self._store_task_transaction(
            task,
            tables,
            raw_sqls,
            blobs,
            store_table,
            lambda raw_sql: self.table_store.store_raw_sql(raw_sql, task, task_info),
            self.blob_store.store_blob,
            store_metadata,
        )

        return m_value

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
    ):
        stage = task.stage
        ctx = RunContext.get()

        stored_tables = []
        stored_blobs = []

        try:
            for table in tables:
                ctx.validate_stage_lock(stage)
                store_table(table)
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
            # Failed - Roll back everything
            for table in stored_tables:
                self.table_store.delete_table_from_transaction(table)
            for blob in stored_blobs:
                self.blob_store.delete_blob_from_transaction(blob)

            ctx.remove_names(stage, tables, blobs)
            raise e

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

        metadata = self.table_store.retrieve_task_metadata(
            task, input_hash, cache_fn_hash
        )
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
        self, task: MaterializingTask
    ) -> (Materializable, TaskMetadata):
        """
        Retrieves the cached output from the most recent execution of a task.

        This retrieval is done based on the name, stage and position hash
        of the task.

        :param task: The materializing task for which to retrieve
            the cached output.
        :return: The output from the task as well as the corresponding metadata.
        :raises CacheError: if no matching task exists in the cache
        """

        # This returns all metadata objects with the same name, stage, AND position
        # hash as `task`. We utilize the position hash to identify a specific
        # task instance, if the same task appears multiple times in a stage.
        metadata = self.table_store.retrieve_all_task_metadata(task)

        if not metadata:
            raise CacheError(
                f"Couldn't find cached output for task '{task}'"
                " with matching position hash."
            )

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
            lambda item: store.dematerialize_item(
                item, as_type=as_type, ctx=run_context
            ),
        )
