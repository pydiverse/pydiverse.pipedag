from __future__ import annotations

import threading
import warnings

import pandas as pd

from pydiverse.pipedag._typing import T
from pydiverse.pipedag.backend.metadata import TaskMetadata
from pydiverse.pipedag.backend.table.base import BaseTableStore, TableHook
from pydiverse.pipedag.core import MaterialisingTask, Schema, Table
from pydiverse.pipedag.errors import CacheError, SchemaError


class DictTableStore(BaseTableStore):
    """
    A very basic table store that stores objects in a dictionary.
    Should only ever be used for testing.
    """

    def __init__(self):
        self.store = dict()
        self.metadata = dict()
        self.run_metadata = dict()
        self.__lock = threading.RLock()

    def create_schema(self, schema: Schema):
        with self.__lock:
            self.store.setdefault(schema.name, {})
            self.store[schema.working_name] = {}

            self.metadata.setdefault(schema.name, {})
            self.run_metadata[schema.name] = {}

    def swap_schema(self, schema: Schema):
        with self.__lock:
            main_schema = self.store[schema.name]
            working_schema = self.store[schema.working_name]
            self.store[schema.name] = working_schema
            self.store[schema.working_name] = main_schema

            # Move metadata from current run into actual metadata store
            self.metadata[schema.name] = self.run_metadata[schema.name]

    def store_table(self, table: Table, lazy: bool):
        schema = table.schema
        with self.__lock:
            if table.name in self.store[schema.working_name]:
                raise Exception(f"Table with name '{table.name}' already in store.")

            hook = self.get_m_table_hook(type(table.obj))
            hook.materialise(self, table, schema.working_name)

    def copy_table_to_working_schema(self, table: Table):
        schema = table.schema
        if schema.did_swap:
            raise SchemaError(
                f"Can't copy table '{table.name}' to working schema."
                f" Schema '{schema.name}' has already been swapped."
            )
        if table.cache_key is None:
            raise ValueError(f"Table cache key can't be None")

        with self.__lock:
            self.store[schema.working_name] = self.store[schema.name]

    def retrieve_table_obj(
        self, table: Table[T], as_type: type[T], from_cache: bool = False
    ) -> T:
        with self.__lock:
            schema = table.schema
            schema_name = schema.name if from_cache else schema.current_name

            hook = self.get_r_table_hook(as_type)
            return hook.retrieve(self, table, schema_name, as_type)

    def store_task_metadata(self, metadata: TaskMetadata, schema: Schema):
        with self.__lock:
            self.run_metadata[schema.name][metadata.cache_key] = metadata

    def copy_task_metadata_to_working_schema(self, task: MaterialisingTask):
        schema = task.schema
        with self.__lock:
            self.run_metadata[schema.name][task.cache_key] = self.metadata[schema.name][
                task.cache_key
            ]

    def retrieve_task_metadata(self, task: MaterialisingTask) -> TaskMetadata:
        with self.__lock:
            try:
                return self.metadata[task.schema.name][task.cache_key]
            except KeyError:
                raise CacheError(
                    "Failed to retrieve metadata for task "
                    f"'{task.name}' with cache key '{task.cache_key}'"
                )


@DictTableStore.register_table(pd)
class PandasTableHook(TableHook[DictTableStore]):
    @classmethod
    def can_materialise(cls, type_) -> bool:
        return issubclass(type_, pd.DataFrame)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == pd.DataFrame

    @classmethod
    def materialise(cls, store, table: Table[pd.DataFrame], schema_name):
        if table.name is not None:
            table.obj.attrs["name"] = table.name
        store.store[schema_name][table.name] = table.obj

    @classmethod
    def retrieve(cls, store, table, schema_name, as_type):
        return store.store[schema_name][table.name].copy()

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        if name := obj.attrs.get("name"):
            return Table(obj, name)
        return super().auto_table(obj)


try:
    import pydiverse.transform as pdt
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    pdt = None


@DictTableStore.register_table(pdt)
class PydiverseTransformTableHook(TableHook[DictTableStore]):
    @classmethod
    def can_materialise(cls, type_) -> bool:
        return issubclass(type_, pdt.Table)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        from pydiverse.transform.eager import PandasTableImpl

        return issubclass(type_, PandasTableImpl)

    @classmethod
    def materialise(cls, store, table: Table[pdt.Table], schema_name):
        from pydiverse.transform.core.verbs import collect

        table.obj = table.obj >> collect()
        return PandasTableHook.materialise(store, table, schema_name)

    @classmethod
    def retrieve(cls, store, table, schema_name, as_type):
        from pydiverse.transform.eager import PandasTableImpl

        df = PandasTableHook.retrieve(store, table, schema_name, pd.DataFrame)
        return pdt.Table(PandasTableImpl(table.name, df))

    @classmethod
    def auto_table(cls, obj: pdt.Table):
        return Table(obj, obj._impl.name)
