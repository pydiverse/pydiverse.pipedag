from __future__ import annotations

import datetime
import itertools
import json
from typing import Callable

import structlog

from pydiverse.pipedag import Blob, Stage, Table, backend
from pydiverse.pipedag._typing import Materializable
from pydiverse.pipedag.context import ConfigContext, RunContext
from pydiverse.pipedag.context.run_context import StageState
from pydiverse.pipedag.errors import DuplicateNameError, StageError
from pydiverse.pipedag.materialize.core import MaterializingTask
from pydiverse.pipedag.materialize.metadata import TaskMetadata
from pydiverse.pipedag.materialize.util import compute_cache_key
from pydiverse.pipedag.materialize.util import json as json_util
from pydiverse.pipedag.util import deep_map


class PipeDAGStore:
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
    ):
        self.table_store = table
        self.blob_store = blob

        self.logger = structlog.get_logger()
        self.json_encoder = json.JSONEncoder(
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
            default=json_util.json_default,
        )
        self.json_decoder = json.JSONDecoder(object_hook=json_util.json_object_hook)

    def close(self):
        """Clean up and close all open resources"""
        self.table_store.close()
        self.blob_store.close()

    #### Stage ####

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

    #### Materialization ####

    def dematerialize_task_inputs(
        self,
        task: MaterializingTask,
        args: tuple[Materializable],
        kwargs: dict[str, Materializable],
    ) -> tuple[tuple, dict]:
        """Loads the inputs for a task from the storage backends

        Traverses the function arguments and replaces all `Table` and
        `Blob` objects with the associated objects stored in the backend.

        :param task: The task for which the arguments should be dematerialized
        :param args: The positional arguments
        :param kwargs: The keyword arguments
        :return: A tuple with the dematerialized args and kwargs
        """

        ctx = RunContext.get()

        def dematerialize_mapper(x):
            if isinstance(x, Table):
                ctx.validate_stage_lock(x.stage)
                return self.table_store.retrieve_table_obj(x, as_type=task.input_type)
            elif isinstance(x, Blob):
                ctx.validate_stage_lock(x.stage)
                return self.blob_store.retrieve_blob(x)
            return x

        d_args = deep_map(args, dematerialize_mapper)
        d_kwargs = deep_map(kwargs, dematerialize_mapper)

        return d_args, d_kwargs

    def materialize_task(
        self,
        task: MaterializingTask,
        value: Materializable,
    ) -> Materializable:
        """Stores the output of a task in the backend

        Traverses the output produced by a task, adds missing metadata,
        materializes all `Table` and `Blob` objects and returns a new
        output object with the required metadata to allow dematerialization.

        :param task: The task instance which produced `value`. Must have
            the correct `cache_key` attribute set.
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
        blobs = []

        config = ConfigContext.get()

        def materialize_mutator(x, counter=itertools.count()):
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

            # Do the materialization
            if isinstance(x, (Table, Blob)):
                x.stage = stage
                x.cache_key = task.cache_key

                # Update name:
                # - If no name has been provided, generate on automatically
                # - If the provided name ends with %%, perform name mangling
                auto_suffix = f"{task.cache_key}_{next(counter):04d}"
                if x.name is None:
                    x.name = task.name + "_" + auto_suffix
                elif x.name.endswith("%%"):
                    x.name = x.name[:-2] + auto_suffix

                ctx.validate_stage_lock(stage)
                if isinstance(x, Table):
                    if x.obj is None:
                        raise TypeError("Underlying table object can't be None")
                    tables.append(x)
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

        # Metadata
        output_json = self.json_encode(m_value)
        metadata = TaskMetadata(
            name=task.name,
            stage=stage.name,
            version=task.version,
            timestamp=datetime.datetime.now(),
            run_id=ctx.run_id,
            cache_key=task.cache_key,
            output_json=output_json,
        )

        # Materialize
        def store_table(table: Table):
            if task.lazy:
                self.table_store.store_table_lazy(table)
            else:
                self.table_store.store_table(table)

        self._check_names(task, tables, blobs)
        self._store_task_transaction(
            task,
            tables,
            blobs,
            store_table,
            lambda blob: self.blob_store.store_blob(blob),
            lambda _: self.table_store.store_task_metadata(metadata, stage),
        )

        return m_value

    def _check_names(
        self, task: MaterializingTask, tables: list[Table], blobs: list[Blob]
    ):
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
        blobs: list[Blob],
        store_table: Callable[[Table], None],
        store_blob: Callable[[Blob], None],
        store_metadata: Callable[[MaterializingTask], None],
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

            ctx.validate_stage_lock(task.stage)
            store_metadata(task)

        except Exception as e:
            # Failed - Roll back everything
            for table in stored_tables:
                self.table_store.delete_table_from_transaction(table)
            for blob in stored_blobs:
                self.blob_store.delete_blob_from_transaction(blob)

            ctx.remove_names(stage, tables, blobs)
            raise e

    #### Cache ####

    def compute_task_cache_key(
        self,
        task: MaterializingTask,
        input_json: str,
    ) -> str:
        """Compute the cache key for a task

        Used by materializing task to create a unique fingerprint to determine
        if the same task has been executed in a previous run with the same
        inputs.

        This task cache key is based on the following values:

        - Task Name
        - Task Version
        - Inputs

        :param task: The task
        :param input_json: The inputs provided to the task serialized as a json
        """
        return compute_cache_key(
            "TASK",
            task.name,
            task.version or "None",
            input_json,
        )

    def retrieve_cached_output(
        self,
        task: MaterializingTask,
    ) -> Materializable:
        """Try to retrieve the cached outputs for a task

        :param task: The materializing task for which to retrieve
            the cached output. Must have the `cache_key` attribute set.
        :raises CacheError: if no matching task exists in the cache
        """

        if task.stage.did_commit:
            raise StageError(f"Stage already committed.")

        metadata = self.table_store.retrieve_task_metadata(task)
        return self.json_decode(metadata.output_json)

    def copy_cached_output_to_transaction_stage(
        self,
        output: Materializable,
        task: MaterializingTask,
    ):
        """Copy the outputs from a cached task into the transaction stage

        If the outputs of a task were successfully retrieved from the cache
        using `retrieve_cached_output`, they and the associated metadata
        must be copied from the base stage to the transaction.

        :raises CacheError: if some values in the output can't be found
            in the cache.
        """

        # Get Tables and Blobs from output
        tables = []
        blobs = []

        def visitor(x):
            if isinstance(x, Table):
                tables.append(x)
            elif isinstance(x, Blob):
                blobs.append(x)
            return x

        deep_map(output, visitor)

        # Materialize
        self._check_names(task, tables, blobs)
        self._store_task_transaction(
            task,
            tables,
            blobs,
            lambda table: self.table_store.copy_table_to_transaction(table),
            lambda blob: self.blob_store.copy_blob_to_transaction(blob),
            lambda task: self.table_store.copy_task_metadata_to_transaction(task),
        )

    #### Utils ####

    def json_encode(self, value: Materializable) -> str:
        """Encode a materializable value as json

        In addition to the default types that python can serialise to json,
        this function can also serialise `Table` and `Blob` objects.
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
