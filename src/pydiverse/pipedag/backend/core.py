from __future__ import annotations

import contextlib
import datetime
import itertools
import json
import threading
import uuid
from collections import defaultdict
from typing import Callable, ContextManager

import prefect.utilities.logging

from pydiverse.pipedag import Blob, Stage, Table, backend, config
from pydiverse.pipedag._typing import Materialisable
from pydiverse.pipedag.backend.lock import LockState
from pydiverse.pipedag.backend.metadata import TaskMetadata
from pydiverse.pipedag.backend.util import compute_cache_key
from pydiverse.pipedag.backend.util import json as json_util
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.context.run.base import StageState
from pydiverse.pipedag.errors import DuplicateNameError, LockError, StageError
from pydiverse.pipedag.materialise.core import MaterialisingTask
from pydiverse.pipedag.util import deepmutate


class PipeDAGStore:
    """Main storage interface for materialising tasks

    Depending on the use case, the store can be configured using different
    backends for storing tables, blobs and managing locks.

    Other than initializing the global `PipeDAGStore` object, the user
    should never have to interact with it. It only serves as a coordinator
    between the different backends, the stages and the materialising tasks.
    """

    def __init__(
        self,
        table: backend.table.BaseTableStore,
        blob: backend.blob.BaseBlobStore,
        lock: backend.lock.BaseLockManager,
    ):
        self.table_store = table
        self.blob_store = blob
        self.lock_manager = lock

        self.logger = prefect.utilities.logging.get_logger("pipeDAG")

        self.json_encoder = json.JSONEncoder(
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
            default=json_util.json_default,
        )
        self.json_decoder = json.JSONDecoder(object_hook=json_util.json_object_hook)

        self.lock_conditions = defaultdict(lambda: threading.Condition())
        self.lock_manager.add_lock_state_listener(self._lock_state_listener)

        # Perform setup operations with lock
        # This is to prevent race conditions, for example, when creating
        # the metadata schema with the SQL backend.
        self.lock_manager.acquire("_pipedag_setup_")
        self.table_store.setup()
        self.lock_manager.release("_pipedag_setup_")

    #### Stage ####

    def register_stage(self, stage: Stage):
        """Used by `Stage` objects to inform the backend about its existence

        As of right now, the mains purpose of this function is to prevent
        creating two stages with the same name, and to be able to retrieve
        a stage object based on its name.
        """
        with self.__lock:
            if stage.name in self.stages:
                raise DuplicateNameError(
                    f"Stage with name '{stage.name}' already exists."
                )
            self.stages[stage.name] = stage

    def init_stage(self, stage: Stage):
        """Initializes the stage in all backends

        This function also acquires a lock on the given stage to prevent
        other flows from modifying the same stage at the same time. Only
        once all tasks that depend on this stage have been executed, will
        this lock be released.

        Don't use this function directly. Instead, use `ensure_stage_is_ready`
        to prevent unnecessary locking.
        """

        # Lock the stage and then create it
        self.acquire_stage_lock(stage)
        self.table_store.init_stage(stage)
        self.blob_store.init_stage(stage)

        # Once stage reference counter hits 0 (this means all tasks that
        # have any task contained in the stage as an upstream dependency),
        # we can release the stage lock.
        stage.set_ref_count_free_handler(self.release_stage_lock)

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
        with stage.commit_context():
            self.validate_lock_state(stage)
            self.table_store.commit_stage(stage)
            self.blob_store.commit_stage(stage)

    #### Materialization ####

    def dematerialise_task_inputs(
        self,
        task: MaterialisingTask,
        args: tuple[Materialisable],
        kwargs: dict[str, Materialisable],
    ) -> tuple[tuple, dict]:
        """Loads the inputs for a task from the storage backends

        Traverses the function arguments and replaces all `Table` and
        `Blob` objects with the associated objects stored in the backend.

        :param task: The task for which the arguments should be dematerialised
        :param args: The positional arguments
        :param kwargs: The keyword arguments
        :return: A tuple with the dematerialised args and kwargs
        """

        def dematerialise_mutator(x):
            if isinstance(x, Table):
                self.validate_lock_state(x.stage)
                return self.table_store.retrieve_table_obj(x, as_type=task.input_type)
            elif isinstance(x, Blob):
                self.validate_lock_state(x.stage)
                return self.blob_store.retrieve_blob(x)
            return x

        d_args = deepmutate(args, dematerialise_mutator)
        d_kwargs = deepmutate(kwargs, dematerialise_mutator)

        return d_args, d_kwargs

    def materialise_task(
        self,
        task: MaterialisingTask,
        value: Materialisable,
    ) -> Materialisable:
        """Stores the output of a task in the backend

        Traverses the output produced by a task, adds missing metadata,
        materialises all `Table` and `Blob` objects and returns a new
        output object with the required metadata to allow dematerialisation.

        :param task: The task instance which produced `value`. Must have
            the correct `cache_key` attribute set.
        :param value: The output of the task. Must be materialisable; this
            means it can only contain the following object types:
            `dict`, `list`, `tuple`,
            `int`, `float`, `str`, `bool`, `None`,
            and PipeDAG's `Table` and `Blob` type.
        :return: A copy of `value` with additional metadata
        """

        ctx = RunContext.get()
        stage = task.stage

        if (state := RunContext.get().get_stage_state(stage)) != StageState.READY:
            raise StageError(
                f"Can't materialise because stage '{stage.name}' is not ready "
                f"(state: {state})."
            )

        tables = []
        blobs = []

        def materialise_mutator(x, counter=itertools.count()):
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

            # Do the materialisation
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

                self.validate_lock_state(stage)
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

        m_value = deepmutate(value, materialise_mutator)

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
        self, task: MaterialisingTask, tables: list[Table], blobs: list[Blob]
    ):
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
        success, t_dup, b_dup = ctx.add_names(tables, blobs)

        if not success:
            tn_dup = [t.name for t in t_dup]
            bn_dup = [b.name for b in b_dup]
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
        task: MaterialisingTask,
        tables: list[Table],
        blobs: list[Blob],
        store_table: Callable[[Table], None],
        store_blob: Callable[[Blob], None],
        store_metadata: Callable[[MaterialisingTask], None],
    ):
        stage = task.stage
        ctx = RunContext.get()

        stored_tables = []
        stored_blobs = []

        try:
            for table in tables:
                self.validate_lock_state(stage)
                store_table(table)
                stored_tables.append(table)
            for blob in blobs:
                self.validate_lock_state(stage)
                store_blob(blob)
                stored_blobs.append(blob)

            self.validate_lock_state(task.stage)
            store_metadata(task)

        except Exception as e:
            # Failed - Roll back everything
            for table in stored_tables:
                self.table_store.delete_table_from_transaction(table)
            for blob in stored_blobs:
                self.blob_store.delete_blob_from_transaction(blob)

            ctx.remove_table_names(tables)
            ctx.remove_blob_names(blobs)
            raise e

    #### Cache ####

    def compute_task_cache_key(
        self,
        task: MaterialisingTask,
        input_json: str,
    ) -> str:
        """Compute the cache key for a task

        Used by materialising task to create a unique fingerprint to determine
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
        task: MaterialisingTask,
    ) -> Materialisable:
        """Try to retrieve the cached outputs for a task

        :param task: The materialising task for which to retrieve
            the cached output. Must have the `cache_key` attribute set.
        :raises CacheError: if no matching task exists in the cache
        """

        if task.stage.did_commit:
            raise StageError(f"Stage already committed.")

        metadata = self.table_store.retrieve_task_metadata(task)
        return self.json_decode(metadata.output_json)

    def copy_cached_output_to_transaction_stage(
        self,
        output: Materialisable,
        task: MaterialisingTask,
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

        deepmutate(output, visitor)

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

    #### Locking ####

    def acquire_stage_lock(self, stage: Stage):
        """Acquires a lock to access the given stage"""
        self.lock_manager.acquire(stage)

    def release_stage_lock(self, stage: Stage):
        """Releases a previously acquired lock on a stage"""
        self.lock_manager.release(stage)

    def validate_lock_state(self, stage: Stage):
        """Validate that a lock is still in the LOCKED state

        Depending on the lock manager, it might be possible that the state
        of a lock can change unexpectedly.

        If a lock becomes unlocked or invalid, we must abort the task (by
        throwing an exception), because we can't guarantee that the data
        it depends on hasn't been changed.
        On the other hand, if we are uncertain about the state (for example
        if connection to the internet is temporarily lost), we must pause
        any task that depends on it and wait until the state of the lock
        becomes known again.

        :raises LockError: if the lock is unlocked
        """
        while True:
            state = self.lock_manager.get_lock_state(stage)
            if state == LockState.LOCKED:
                return
            elif state == LockState.UNLOCKED:
                raise LockError(f"Lock for stage '{stage.name}' is unlocked.")
            elif state == LockState.INVALID:
                raise LockError(f"Lock for stage '{stage.name}' is invalid.")
            elif state == LockState.UNCERTAIN:
                self.logger.info(
                    f"Waiting for stage '{stage.name}' lock state to become known"
                    " again..."
                )
                cond = self.lock_conditions[stage]
                with cond:
                    cond.wait()
            else:
                raise ValueError(f"Invalid state '{state}'.")

    def _lock_state_listener(
        self, stage: Stage, old_state: LockState, new_state: LockState
    ):
        """Internal listener that gets notified when the state of a lock changes"""
        if not isinstance(stage, Stage):
            return

        # Notify all waiting threads that the lock state has changed
        cond = self.lock_conditions[stage]
        with cond:
            cond.notify_all()

        # Logging
        if new_state == LockState.UNCERTAIN:
            self.logger.warning(
                f"Lock for stage '{stage.name}' transitioned to UNCERTAIN state."
            )
        if old_state == LockState.UNCERTAIN and new_state == LockState.LOCKED:
            self.logger.info(
                f"Lock for stage '{stage.name}' is still LOCKED (after being"
                " UNCERTAIN)."
            )
        if old_state == LockState.UNCERTAIN and new_state == LockState.INVALID:
            self.logger.error(f"Lock for stage '{stage.name}' has become INVALID.")

    #### Utils ####

    def json_encode(self, value: Materialisable) -> str:
        """Encode a materialisable value as json

        In addition to the default types that python can serialise to json,
        this function can also serialise `Table` and `Blob` objects.
        The only caveat is, that python's json module doesn't differentiate
        between lists and tuples, which means it is impossible to
        differentiate the two.
        """
        return self.json_encoder.encode(value)

    def json_decode(self, value: str) -> Materialisable:
        """Decode a materialisable value as json

        Counterpart for the `json_encode` function. Can decode `Table` and
        `Blob` objects.
        """
        return self.json_decoder.decode(value)
