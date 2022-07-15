import threading
import uuid
from abc import ABC, abstractmethod
from typing import Callable, Type

import pandas as pd
import sqlalchemy as sa

from pdpipedag._typing import T
from pdpipedag.core.materialise import MaterialisingTask
from pdpipedag.core.metadata import TaskMetadata
from pdpipedag.core.schema import Schema
from pdpipedag.core.table import Table
from pdpipedag.errors import CacheError
from pdpipedag.errors import SchemaError
from .sql import CreateSchema, DropSchema, RenameSchema, CopyTable


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
    def retrieve_table_obj(self, table: Table, as_type: Type[T], from_cache: bool = False) -> T:
        ...

    @abstractmethod
    def store_task_metadata(self, metadata: TaskMetadata):
        ...

    @abstractmethod
    def copy_task_metadata_to_working_schema(self, task: MaterialisingTask):
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

    def retrieve_table_obj(self, table: Table[T], as_type: Type[T], from_cache: bool = False) -> T:
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

    def copy_task_metadata_to_working_schema(self, task: MaterialisingTask):
        schema = task.schema
        with self.__lock:
            self.run_metadata[schema.name][task.cache_key] = self.metadata[schema.name][task.cache_key]

    def retrieve_task_metadata(self, task, cache_key: str) -> TaskMetadata:
        with self.__lock:
            try:
                return self.metadata[task.schema.name][cache_key]
            except KeyError:
                raise CacheError(
                    f"Failed to retrieve metadata for task "
                    f"'{task.name}' with cache key '{cache_key}'")


class SQLTableStore(BaseTableStore):

    METADATA_SCHEMA = 'pipedag_metadata'

    def __init__(self, engine: sa.engine.Engine):
        self.engine = engine

        # Set up metadata tables and schema
        from sqlalchemy import Column, BigInteger, String, DateTime, Boolean

        self.sql_metadata = sa.MetaData()
        self.tasks_table = sa.Table(
            'tasks', self.sql_metadata,

            Column('id', BigInteger, primary_key = True, autoincrement = True),
            Column('name', String),
            Column('schema', String),
            Column('version', String),
            Column('timestamp', DateTime),
            Column('run_id', String(20)),     # TODO: Replace with appropriate type
            Column('cache_key', String(20)),  # TODO: Replace with appropriate type
            Column('output_json', String),
            Column('in_working_schema', Boolean),

            schema = self.METADATA_SCHEMA,
        )

        with self.engine.connect() as conn:
            conn.execute(CreateSchema(self.METADATA_SCHEMA, if_not_exists = True))
            self.sql_metadata.create_all(conn)

    def create_schema(self, schema: Schema):
        cs_main = CreateSchema(schema.name, if_not_exists = True)
        ds_working = DropSchema(schema.working_name, if_exists = True, cascade = True)
        cs_working = CreateSchema(schema.working_name, if_not_exists = True)

        with self.engine.connect() as conn:
            conn.execute(cs_main)
            conn.execute(ds_working)
            conn.execute(cs_working)

            conn.execute(
                self.tasks_table.delete()
                .where(self.tasks_table.c.schema == schema.name)
                .where(self.tasks_table.c.in_working_schema == True)
            )

    def swap_schema(self, schema: Schema):
        tmp = schema.name + '__tmp_swap'
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(RenameSchema(schema.working_name, tmp))
                conn.execute(RenameSchema(schema.name, schema.working_name))
                conn.execute(RenameSchema(tmp, schema.name))
                conn.execute(DropSchema(tmp, if_exists = True, cascade = True))
                conn.execute(DropSchema(schema.working_name, if_exists = True, cascade = True))

                conn.execute(
                    self.tasks_table.delete()
                    .where(self.tasks_table.c.schema == schema.name)
                    .where(self.tasks_table.c.in_working_schema == False)
                )
                conn.execute(
                    self.tasks_table.update()
                    .where(self.tasks_table.c.schema == schema.name)
                    .values(in_working_schema = False)
                )

    def store_table(self, table: Table):
        schema = table.schema
        if schema is None:
            raise ValueError(f"Table schema can't be None.")
        if schema.did_swap:
            raise SchemaError(f"Can't add new table to Schema '{schema.name}'. Schema has already been swapped.")
        if not isinstance(table.name, str):
            raise TypeError(f"Table name must be of instance 'str' not '{type(table.name).__name__}'.")

        obj = table.obj
        assert obj is not None
        if isinstance(obj, pd.DataFrame):
            obj.to_sql(
                table.name,
                self.engine,
                schema = schema.working_name,
                if_exists = 'replace',
                index = False,
            )

    def copy_table_to_working_schema(self, table: Table):
        schema = table.schema
        with self.engine.connect() as conn:
            conn.execute(CopyTable(table.name, schema.name, schema.working_name))

    def retrieve_table_obj(self, table: Table, as_type: Type[T], from_cache: bool = False) -> T:
        schema = table.schema
        schema_name = schema.name if from_cache else schema.current_name

        if as_type == pd.DataFrame:
            with self.engine.connect() as conn:
                df = pd.read_sql_table(table.name, conn, schema = schema_name)
                print(df)
                return df

    def store_task_metadata(self, metadata: TaskMetadata):
        with self.engine.connect() as conn:
            conn.execute(
                self.tasks_table.insert().values(
                    name = metadata.name,
                    schema = metadata.schema,
                    version = metadata.version,
                    timestamp = metadata.timestamp,
                    run_id = metadata.run_id,
                    cache_key = metadata.cache_key,
                    output_json = metadata.output_json,
                    in_working_schema = True,
                )
            )

    def copy_task_metadata_to_working_schema(self, task: MaterialisingTask):
        with self.engine.connect() as conn:
            metadata = conn.execute(
                self.tasks_table.select()
                .where(self.tasks_table.c.schema == task.schema.name)
                .where(self.tasks_table.c.cache_key == task.cache_key)
                .where(self.tasks_table.c.in_working_schema == False)
            ).mappings().one()

            metadata_copy = dict(metadata)
            metadata_copy['in_working_schema'] = True
            del metadata_copy['id']

            conn.execute(
                self.tasks_table.insert().values(**metadata_copy)
            )

    def retrieve_task_metadata(self, task: MaterialisingTask, cache_key: str) -> TaskMetadata:
        with self.engine.connect() as conn:
            result = conn.execute(
                self.tasks_table.select()
                .where(self.tasks_table.c.schema == task.schema.name)
                .where(self.tasks_table.c.cache_key == task.cache_key)
                .where(self.tasks_table.c.in_working_schema == False)
            ).mappings().one_or_none()

        if result is None:
            raise CacheError(f"Couldn't retrieve task for cache key {cache_key}")

        return TaskMetadata(
            name = result.name,
            schema = result.schema,
            version = result.version,
            timestamp = result.timestamp,
            run_id = result.run_id,
            cache_key = result.cache_key,
            output_json = result.output_json,
        )
