import threading
import typing
from abc import ABC, abstractmethod

import sqlalchemy as sa

from pdpipedag._typing import T
from pdpipedag.core.materialise import MaterialisingTask
from pdpipedag.core.metadata import TaskMetadata
from pdpipedag.core.schema import Schema
from pdpipedag.core.table import Table
from pdpipedag.errors import CacheError
from pdpipedag.errors import SchemaError


class BaseTableStore(ABC):

    @abstractmethod
    def create_schema(self, schema: Schema):
        """Creates a (new) schema."""
        ...

    @abstractmethod
    def swap_schema(self, schema: Schema):
        ...

    @abstractmethod
    def store_table(self, table: Table):
        ...

    @abstractmethod
    def copy_table_to_working_schema(self, table: Table):
        ...

    @abstractmethod
    def retrieve_table_obj(self, table: Table, as_type: typing.Type[T], from_cache: bool = False) -> T:
        ...

    @abstractmethod
    def store_task_metadata(self, metadata: TaskMetadata):
        ...

    @abstractmethod
    def retrieve_task_metadata(self, task: MaterialisingTask, cache_key: str) -> TaskMetadata:
        ...


class DictTableStore(BaseTableStore):
    """
    A very basic table store that stores objects in a dictionary.
    Should only ever be used for testing.
    """

    def __init__(self):
        self.store = dict()
        self.metadata = dict()
        self.run_metadata = dict()
        self.__lock = threading.Lock()

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

    def store_table(self, table: Table):
        schema = table.schema
        if schema is None:
            raise ValueError(f"Table schema can't be None.")
        if schema.did_swap:
            raise SchemaError(f"Can't add new table to Schema '{schema.name}'. Schema has already been swapped.")
        if not isinstance(table.name, str):
            raise TypeError(f"Table name must be of instance 'str' not '{type(table.name).__name__}'.")

        with self.__lock:
            if table.name in self.store[schema.working_name]:
                raise Exception(f"Table with name '{table.name}' already in store.")
            self.store[schema.working_name][table.name] = table.obj

    def copy_table_to_working_schema(self, table: Table):
        schema = table.schema
        if schema.did_swap:
            raise SchemaError(f"Can't copy table '{table.name}' to working schema. Schema '{schema.name}' has already been swapped.")
        if table.cache_key is None:
            raise ValueError(f"Table cache key can't be None.")

        with self.__lock:
            self.store[schema.working_name] = self.store[schema.name]
            self.run_metadata[schema.name][table.cache_key] = self.metadata[schema.name][table.cache_key]

    def retrieve_table_obj(self, table: Table[T], as_type: typing.Type[T], from_cache: bool = False) -> T:
        with self.__lock:
            if from_cache:
                obj = self.store[table.schema.name][table.name]
            else:
                obj = self.store[table.schema.current_name][table.name]

        if isinstance(obj, as_type):
            return obj.copy()

        raise Exception(f"{type(self).__name__} can't convert from type {type(obj)} to {as_type}.")

    def store_task_metadata(self, metadata: TaskMetadata):
        with self.__lock:
            self.run_metadata[metadata.schema][metadata.cache_key] = metadata

    def retrieve_task_metadata(self, task, cache_key: str) -> TaskMetadata:
        with self.__lock:
            try:
                return self.metadata[task.schema.name][cache_key]
            except KeyError:
                raise CacheError(
                    f"Failed to retrieve metadata for task "
                    f"'{task.name}' with cache key '{cache_key}'")


class SQLTableStore(BaseTableStore):

    def __init__(self, engine: sa.engine.Engine):
        self.engine = engine

    def create_schema(self, schema: Schema):
        ...

    def swap_schema(self, schema: Schema):
        ...

    def store_table(self, table: Table):
        ...

    def retrieve_table(self, table: Table):
        ...
