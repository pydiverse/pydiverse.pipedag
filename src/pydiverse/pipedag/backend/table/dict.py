from __future__ import annotations

import warnings

import pandas as pd

from pydiverse.pipedag.backend.metadata import LazyTableMetadata, TaskMetadata
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
        self.w_metadata = dict()

        self.lazy_table_metadata = dict()
        self.w_lazy_table_metadata = dict()

    def create_schema(self, schema: Schema):
        self.store.setdefault(schema.name, {})
        self.store[schema.working_name] = {}

        self.metadata.setdefault(schema, {})
        self.w_metadata[schema] = {}

        self.lazy_table_metadata.setdefault(schema, {})
        self.w_lazy_table_metadata[schema] = {}

    def swap_schema(self, schema: Schema):
        main_schema = self.store[schema.name]
        working_schema = self.store[schema.working_name]
        self.store[schema.name] = working_schema
        self.store[schema.working_name] = main_schema

        # Move metadata from working schema into actual metadata store
        self.metadata[schema] = self.w_metadata[schema]
        self.lazy_table_metadata[schema] = self.w_lazy_table_metadata[schema]

    def copy_table_to_working_schema(self, table: Table):
        schema = table.schema
        if schema.did_swap:
            raise SchemaError(
                f"Can't copy table '{table.name}' to working schema."
                f" Schema '{schema.name}' has already been swapped."
            )
        if table.cache_key is None:
            raise ValueError(f"Table cache key can't be None")

        self.store[schema.working_name] = self.store[schema.name]

    def copy_lazy_table_to_working_schema(
        self, metadata: LazyTableMetadata, table: Table
    ):
        if (metadata.schema not in self.store) or (
            metadata.name not in self.store[metadata.schema]
        ):
            raise CacheError(
                f"Can't copy lazy table '{metadata.name}' (schema:"
                f" '{metadata.schema}') to working schema because no such table"
                " exists."
            )

        self.store[table.schema.working_name][table.name] = self.store[metadata.schema][
            metadata.name
        ]

    def delete_table_from_working_schema(self, table: Table):
        try:
            self.store[table.schema.working_name].pop(table.name)
        except KeyError:
            return

    def store_task_metadata(self, metadata: TaskMetadata, schema: Schema):
        self.w_metadata[schema][metadata.cache_key] = metadata

    def copy_task_metadata_to_working_schema(self, task: MaterialisingTask):
        schema = task.schema
        self.w_metadata[schema][task.cache_key] = self.metadata[schema][task.cache_key]

    def retrieve_task_metadata(self, task: MaterialisingTask) -> TaskMetadata:
        try:
            return self.metadata[task.schema][task.cache_key]
        except KeyError:
            raise CacheError(
                "Failed to retrieve metadata for task "
                f"'{task.name}' with cache key '{task.cache_key}'"
            )

    def store_lazy_table_metadata(self, metadata: LazyTableMetadata):
        self.w_lazy_table_metadata[metadata.schema][metadata.cache_key] = metadata

    def retrieve_lazy_table_metadata(
        self, cache_key: str, schema: Schema
    ) -> LazyTableMetadata:
        try:
            return self.lazy_table_metadata[schema.name][cache_key]
        except (TypeError, KeyError):
            raise CacheError


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
