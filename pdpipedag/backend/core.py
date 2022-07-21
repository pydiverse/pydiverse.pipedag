from __future__ import annotations

import datetime
import hashlib
import itertools
import json
import threading
import uuid
from collections import defaultdict

import prefect.utilities.logging

from pdpipedag import backend
from pdpipedag._typing import Materialisable
from pdpipedag.backend.lock import LockState
from pdpipedag.backend.metadata import TaskMetadata
from pdpipedag.backend.util import json as json_util
from pdpipedag.core import Schema, Table, Blob, MaterialisingTask
from pdpipedag.errors import SchemaError, LockError
from pdpipedag.util import deepmutate


class PipeDAGStore:
    """Main storage interface for materialising tasks."""

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
        self.run_id = uuid.uuid4().hex[:20]
        self.logger = prefect.utilities.logging.get_logger('pipeDAG')

        self.json_encoder = json.JSONEncoder(
            ensure_ascii = False,
            allow_nan = False,
            separators = (',', ':'),
            sort_keys = True,
            default = json_util.json_default,
        )
        self.json_decoder = json.JSONDecoder(object_hook = json_util.json_object_hook)

        self.lock_conditions = defaultdict(lambda: threading.Condition())
        self.lock_manager.add_lock_state_listener(self._lock_state_listener)

    #### Schema ####

    def register_schema(self, schema: Schema):
        with self.__lock:
            if schema.name in self.schemas:
                raise SchemaError(f"Schema with name '{schema.name}' already exists.")
            self.schemas[schema.name] = schema

    def create_schema(self, schema: Schema):
        with self.__lock:
            if schema in self.created_schemas:
                raise SchemaError(f"Schema '{schema.name}' has already been created.")
            if schema.name not in self.schemas:
                raise SchemaError(f"Can't create schema '{schema.name}' because it hasn't been registered.")
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
        with self.__lock:
            if schema in self.created_schemas:
                return
            self.create_schema(schema)

    def swap_schema(self, schema: Schema):
        """Swap the working schema with the base schema."""
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

        def dematerialise_mutator(x):
            if isinstance(x, Table):
                self.validate_lock_state(x.schema)
                return self.table_store.retrieve_table_obj(x, as_type = task.input_type)
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

        schema = task.schema
        if schema not in self.created_schemas:
            raise SchemaError(f"Can't materialise because schema '{schema.name}' has not been created.")

        def materialise_mutator(x, tbl_id = itertools.count()):
            if isinstance(x, (Table, Blob)):
                # TODO: Don't overwrite name unless it is None
                x.schema = schema
                x.name = f'{task.original_name}_{task.cache_key}_{next(tbl_id):04d}'
                x.cache_key = task.cache_key

                self.validate_lock_state(schema)
                if isinstance(x, Table):
                    self.table_store.store_table(x, lazy = task.lazy)
                elif isinstance(x, Blob):
                    self.blob_store.store_blob(x)
                else:
                    raise NotImplementedError

            return x

        # Materialise
        m_value = deepmutate(value, materialise_mutator)

        # Metadata
        output_json = self.json_encode(m_value)
        metadata = TaskMetadata(
            name = task.original_name,
            schema = schema.name,
            version = task.version,
            timestamp = datetime.datetime.now(),
            run_id = self.run_id,
            cache_key = task.cache_key,
            output_json = output_json,
        )

        self.validate_lock_state(schema)
        self.table_store.store_task_metadata(metadata, schema)

        return m_value

    #### Cache ####

    def compute_task_cache_key(
            self,
            task: MaterialisingTask,
            input_json: str,
    ) -> str:
        """Compute the cache key for a task.

        This task hash is based on the following values:
        - Task Name
        - Task Version
        - Inputs

        :param task: The task.
        :param input_json: The inputs provided to the task serialized as a json.
        :return: A sha256 hex digest.
        """

        # Maybe look into `dask.base.tokenize`

        v = (
            'PYDIVERSE-PIPEDAG-TASK',
            task.original_name,
            task.version or 'None',
            input_json,
        )

        v_str = '|'.join(v)
        v_bytes = v_str.encode('utf8')

        v_hash = hashlib.sha256(v_bytes)
        return v_hash.hexdigest()[:20]  # Provides 40 bit of collision resistance

    def retrieve_cached_output(
            self,
            task: MaterialisingTask,
    ) -> Materialisable:

        if task.schema.did_swap:
            raise SchemaError(f"Schema already swapped.")

        metadata = self.table_store.retrieve_task_metadata(task)
        return self.json_decode(metadata.output_json)

    def copy_cached_output_to_working_schema(
            self,
            output: Materialisable,
            task: MaterialisingTask,
    ):

        def visiting_mutator(x):
            if isinstance(x, Table):
                self.validate_lock_state(task.schema)
                self.table_store.copy_table_to_working_schema(x)
            elif isinstance(x, Blob):
                self.validate_lock_state(task.schema)
                self.blob_store.copy_blob_to_working_schema(x)
            return x

        deepmutate(output, visiting_mutator)

        self.validate_lock_state(task.schema)
        self.table_store.copy_task_metadata_to_working_schema(task)

    #### Locking ####

    def acquire_schema_lock(self, schema: Schema):
        self.lock_manager.acquire_schema(schema)

    def release_schema_lock(self, schema: Schema):
        self.lock_manager.release_schema(schema)

    def validate_lock_state(self, schema: Schema):
        while True:
            state = self.lock_manager.get_lock_state(schema)
            if state == LockState.LOCKED:
                return
            elif state == LockState.UNLOCKED:
                raise LockError(f"Lock for schema '{schema.name}' is unlocked.")
            elif state == LockState.INVALID:
                raise LockError(f"Lock for schema '{schema.name}' is invalid.")
            elif state == LockState.UNCERTAIN:
                self.logger.info(f"Waiting for schema '{schema.name}' lock state to become known again...")
                cond = self.lock_conditions[schema]
                with cond:
                    cond.wait()
            else:
                raise ValueError(f"Invalid state '{state}'.")

    def _lock_state_listener(
            self,
            schema: Schema,
            old_state: LockState,
            new_state: LockState
    ):
        # Notify all waiting threads that the lock state has changed
        cond = self.lock_conditions[schema]
        with cond:
            cond.notify_all()

        # Logging
        if new_state == LockState.UNCERTAIN:
            self.logger.warning(f"Lock for schema '{schema.name}' transitioned to UNCERTAIN state.")
        if old_state == LockState.UNCERTAIN and new_state == LockState.LOCKED:
            self.logger.info(f"Lock for schema '{schema.name}' is still LOCKED (after being UNCERTAIN).")
        if old_state == LockState.UNCERTAIN and new_state == LockState.INVALID:
            self.logger.error(f"Lock for schema '{schema.name}' has become INVALID.")

    #### Utils ####

    def json_encode(self, value: Materialisable) -> str:
        return self.json_encoder.encode(value)

    def json_decode(self, value: str) -> Materialisable:
        return self.json_decoder.decode(value)

    def _reset(self):
        self.schemas.clear()
        self.created_schemas.clear()
        self.run_id = uuid.uuid4().hex[:20]
