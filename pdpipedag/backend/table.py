import threading
import typing
from abc import ABC, abstractmethod

import sqlalchemy as sa

from pdpipedag._typing import T
from pdpipedag.core.table import Table
from pdpipedag.core.schema import Schema
from pdpipedag.errors import SchemaError


class BaseTableStorage(ABC):

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
    def retrieve_table_obj(self, table: Table, as_type: typing.Type[T]) -> T:
        ...


class DictTableStorage(BaseTableStorage):
    """
    A very basic table store that stores objects in a dictionary.
    Should only ever be used for testing.
    """

    def __init__(self):
        self.store = dict()
        self.__lock = threading.Lock()

    def create_schema(self, schema: Schema):
        with self.__lock:
            self.store.setdefault(schema.name, dict())
            self.store[schema.working_name] = {}

    def swap_schema(self, schema: Schema):
        with self.__lock:
            main_schema = self.store[schema.name]
            working_schema = self.store[schema.working_name]

            self.store[schema.name] = working_schema
            self.store[schema.working_name] = main_schema

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

    def retrieve_table_obj(self, table: Table[T], as_type: typing.Type[T]) -> T:
        with self.__lock:
            obj = self.store[table.schema.current_name][table.name]

        if isinstance(obj, as_type):
            return obj

        raise Exception(f"{type(self).__name__} can't convert from type {type(obj)} to {as_type}.")


class SQLTableStorage(BaseTableStorage):

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
