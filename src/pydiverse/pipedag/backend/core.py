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

from pydiverse.pipedag import backend, config
from pydiverse.pipedag._typing import Materialisable
from pydiverse.pipedag.backend.lock import LockState
from pydiverse.pipedag.backend.metadata import TaskMetadata
from pydiverse.pipedag.backend.util import compute_cache_key
from pydiverse.pipedag.backend.util import json as json_util
from pydiverse.pipedag.core import Blob, MaterialisingTask, Schema, Table
from pydiverse.pipedag.errors import DuplicateNameError, LockError, SchemaError
from pydiverse.pipedag.util import deepmutate


class PipeDAGStore:
    """Main storage interface for materialising tasks

    Depending on the use case, the store can be configured using different
    backends for storing tables, blobs and managing locks.

    Other than initializing the global `PipeDAGStore` object, the user
    should never have to interact with it. It only serves as a coordinator
    between the different backends, the schemas and the materialising tasks.
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

        self.__lock = threading.RLock()
        self.schemas: dict[str, Schema] = dict()
        self.created_schemas: set[Schema] = set()
        self.table_names: defaultdict[Schema, set[str]] = defaultdict(set)
        self.blob_names: defaultdict[Schema, set[str]] = defaultdict(set)
        self.run_id = uuid.uuid4().hex[:20]
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

    #### Schema ####

    def register_schema(self, schema: Schema):
        """Used by `Schema` objects to inform the backend about its existence

        As of right now, the mains purpose of this function is to prevent
        creating two schemas with the same name, and to be able to retrieve
        a schema object based on its name.
        """
        with self.__lock:
            if schema.name in self.schemas:
                raise DuplicateNameError(
                    f"Schema with name '{schema.name}' already exists."
                )
            self.schemas[schema.name] = schema

    def create_schema(self, schema: Schema):
        """Creates the schema in all backends

        This function also acquires a lock on the given schema to prevent
        other flows from modifying the same schema at the same time. Only
        once all tasks that depend on this schema have been executed, will
        this lock be released.

        Don't use this function directly. Instead, use `ensure_schema_is_ready`
        to prevent unnecessary locking.
        """
        with self.__lock:
            if schema in self.created_schemas:
                raise SchemaError(f"Schema '{schema.name}' has already been created.")
            if schema.name not in self.schemas:
                raise SchemaError(
                    f"Can't create schema '{schema.name}' because it hasn't been"
                    " registered."
                )
            self.created_schemas.add(schema)

        # Lock the schema and then create it
        self.acquire_schema_lock(schema)
        self.table_store.create_schema(schema)
        self.blob_store.create_schema(schema)

        # Once schema reference counter hits 0 (this means all tasks that
        # have any task contained in the schema as an upstream dependency),
        # we can release the schema lock.
        schema._set_ref_count_free_handler(self.release_schema_lock)

    def ensure_schema_is_ready(self, schema: Schema):
        """Creates a schema if it hasn't been created yet

        This allows the creation of schemas in a lazy way. This ensures
        that a schema only gets locked right before it is needed.
        """
        with self.__lock:
            if schema in self.created_schemas:
                return
            self.create_schema(schema)

    def swap_schema(self, schema: Schema):
        """Swap the working schema with the base schema"""
        with schema.perform_swap():
            self.validate_lock_state(schema)
            self.table_store.swap_schema(schema)
            self.blob_store.swap_schema(schema)

    #### Task ####

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
                self.validate_lock_state(x.schema)
                return self.table_store.retrieve_table_obj(x, as_type=task.input_type)
            elif isinstance(x, Blob):
                self.validate_lock_state(x.schema)
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

        schema = task.schema
        if schema not in self.created_schemas:
            raise SchemaError(
                f"Can't materialise because schema '{schema.name}' has not been"
                " created."
            )
        if schema.did_swap:
            raise SchemaError(
                f"Can't add new table to Schema '{schema.name}'."
                " Schema has already been swapped."
            )

        def materialise_mutator(x, tbl_id=itertools.count()):
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
                x.schema = schema
                x.cache_key = task.cache_key

                # Update name:
                # - If no name has been provided, generate on automatically
                # - If the provided name ends with %%, perform name mangling
                auto_suffix = f"{task.cache_key}_{next(tbl_id):04d}"
                if x.name is None:
                    x.name = task.original_name + "_" + auto_suffix
                elif x.name.endswith("%%"):
                    x.name = x.name[:-2] + auto_suffix

                self.validate_lock_state(schema)
                if isinstance(x, Table):
                    if x.obj is None:
                        raise TypeError("Underlying table object can't be None")

                    name_checker(x)
                    if task.lazy:
                        self.table_store.store_table_lazy(x)
                    else:
                        self.table_store.store_table(x)
                elif isinstance(x, Blob):
                    if task.lazy:
                        raise NotImplementedError(
                            "Can't use Blobs with lazy tasks. Invalidation of the"
                            " downstream dependencies is not implemented."
                        )

                    name_checker(x)
                    self.blob_store.store_blob(x)
                else:
                    raise NotImplementedError

            return x

        # Materialise
        with self.materialisation_context() as name_checker:
            m_value = deepmutate(value, materialise_mutator)

        # Metadata
        output_json = self.json_encode(m_value)
        metadata = TaskMetadata(
            name=task.original_name,
            schema=schema.name,
            version=task.version,
            timestamp=datetime.datetime.now(),
            run_id=self.run_id,
            cache_key=task.cache_key,
            output_json=output_json,
        )

        self.validate_lock_state(schema)
        self.table_store.store_task_metadata(metadata, schema)

        return m_value

    @contextlib.contextmanager
    def materialisation_context(self) -> ContextManager[Callable[[Table | Blob], None]]:
        """Context manager for materialisation

        All materialisation operations should be wrapped using this context
        manager. It does the following things:

        - It returns a name checker function which, when called with either
          a Table or Blob, checks if an object with the same name already
          exists in the schema. If it does, it raises a `DuplicateNameError`.

        - If an exception is raised inside the context, all objects that have
          been materialised (given that the name checker function was called
          with them), get deleted again.

        The deletion behaviour is important, because when copying tables from
        the cache to the working schema, we want to be able to delete
        """
        stored_objects: list[Table | Blob] = []

        def name_checker(obj: Table | Blob):
            if isinstance(obj, Table):
                name_store = self.table_names
            elif isinstance(obj, Blob):
                name_store = self.blob_names
            else:
                raise TypeError

            with self.__lock:
                if obj.name in name_store[obj.schema]:
                    raise DuplicateNameError(
                        f"{type(obj).__name__} with name '{obj.name}' already"
                        " exists in schema '{obj.schema.name}'."
                        " To enable automatic name mangling,"
                        " you can add '%%' at the end of the name."
                    )

                stored_objects.append(obj)
                name_store[obj.schema].add(obj.name)

        try:
            yield name_checker
        except Exception as e:
            # Clean up
            with self.__lock:
                for obj in stored_objects:
                    if isinstance(obj, Table):
                        self.table_names[obj.schema].remove(obj.name)
                        self.table_store.delete_table_from_working_schema(obj)
                    elif isinstance(obj, Blob):
                        self.blob_names[obj.schema].remove(obj.name)
                        self.blob_store.delete_blob_from_working_schema(obj)
                    else:
                        raise TypeError
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
            task.original_name,
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

        if task.schema.did_swap:
            raise SchemaError(f"Schema already swapped.")

        metadata = self.table_store.retrieve_task_metadata(task)
        return self.json_decode(metadata.output_json)

    def copy_cached_output_to_working_schema(
        self,
        output: Materialisable,
        task: MaterialisingTask,
    ):
        """Copy the outputs from a cached task into the working schema

        If the outputs of a task were successfully retrieved from the cache
        using `retrieve_cached_output`, they and the associated metadata
        must be copied from the base schema to the working schema.

        :raises CacheError: if some values in the output can't be found
            in the cache.
        """

        def visiting_mutator(x):
            if isinstance(x, Table):
                name_checker(x)
                self.validate_lock_state(task.schema)
                self.table_store.copy_table_to_working_schema(x)
            elif isinstance(x, Blob):
                name_checker(x)
                self.validate_lock_state(task.schema)
                self.blob_store.copy_blob_to_working_schema(x)
            return x

        with self.materialisation_context() as name_checker:
            deepmutate(output, visiting_mutator)

        self.validate_lock_state(task.schema)
        self.table_store.copy_task_metadata_to_working_schema(task)

    #### Locking ####

    def acquire_schema_lock(self, schema: Schema):
        """Acquires a lock to access the given schema"""
        self.lock_manager.acquire(schema)

    def release_schema_lock(self, schema: Schema):
        """Releases a previously acquired lock on a schema"""
        self.lock_manager.release(schema)

    def validate_lock_state(self, schema: Schema):
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
            state = self.lock_manager.get_lock_state(schema)
            if state == LockState.LOCKED:
                return
            elif state == LockState.UNLOCKED:
                raise LockError(f"Lock for schema '{schema.name}' is unlocked.")
            elif state == LockState.INVALID:
                raise LockError(f"Lock for schema '{schema.name}' is invalid.")
            elif state == LockState.UNCERTAIN:
                self.logger.info(
                    f"Waiting for schema '{schema.name}' lock state to become known"
                    " again..."
                )
                cond = self.lock_conditions[schema]
                with cond:
                    cond.wait()
            else:
                raise ValueError(f"Invalid state '{state}'.")

    def _lock_state_listener(
        self, schema: Schema, old_state: LockState, new_state: LockState
    ):
        """Internal listener that gets notified when the state of a lock changes"""
        if not isinstance(schema, Schema):
            return

        # Notify all waiting threads that the lock state has changed
        cond = self.lock_conditions[schema]
        with cond:
            cond.notify_all()

        # Logging
        if new_state == LockState.UNCERTAIN:
            self.logger.warning(
                f"Lock for schema '{schema.name}' transitioned to UNCERTAIN state."
            )
        if old_state == LockState.UNCERTAIN and new_state == LockState.LOCKED:
            self.logger.info(
                f"Lock for schema '{schema.name}' is still LOCKED (after being"
                " UNCERTAIN)."
            )
        if old_state == LockState.UNCERTAIN and new_state == LockState.INVALID:
            self.logger.error(f"Lock for schema '{schema.name}' has become INVALID.")

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

    def _reset(self):
        for schema in self.schemas.values():
            try:
                self.lock_manager.release(schema)
            except LockError:
                pass

        self.schemas.clear()
        self.created_schemas.clear()
        self.table_names.clear()
        self.blob_names.clear()
        self.run_id = uuid.uuid4().hex[:20]
