from __future__ import annotations

import hashlib
import threading
from abc import ABC, abstractmethod
from typing import Type

import pandas as pd
import prefect
import sqlalchemy as sa
import sqlalchemy.exc

from pdpipedag._typing import T
from pdpipedag.backend.metadata import TaskMetadata, LazyTableMetadata
from pdpipedag.core import Schema, Table, MaterialisingTask
from pdpipedag.errors import CacheError, SchemaError
from .util.sql_ddl import CreateSchema, DropSchema, RenameSchema, CopyTable, CreateTableAsSelect

__all__ = [
    'BaseTableStore',
    'DictTableStore',
    'SQLTableStore',
]


class BaseTableStore(ABC):

    @abstractmethod
    def create_schema(self, schema: Schema):
        """Creates a (new) schema."""
        ...

    @abstractmethod
    def swap_schema(self, schema: Schema):
        ...

    @abstractmethod
    def store_table(self, table: Table, lazy: bool):
        ...

    @abstractmethod
    def copy_table_to_working_schema(self, table: Table):
        ...

    @abstractmethod
    def retrieve_table_obj(self, table: Table, as_type: Type[T], from_cache: bool = False) -> T:
        ...

    @abstractmethod
    def store_task_metadata(self, metadata: TaskMetadata, schema: Schema):
        ...

    @abstractmethod
    def copy_task_metadata_to_working_schema(self, task: MaterialisingTask):
        ...

    @abstractmethod
    def retrieve_task_metadata(self, task: MaterialisingTask) -> TaskMetadata:
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

    def store_table(self, table: Table, lazy: bool):
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
            raise SchemaError(
                f"Can't copy table '{table.name}' to working schema. Schema '{schema.name}' has already been swapped.")
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

        raise TypeError(f"{type(self).__name__} can't convert from type {type(obj)} to {as_type}.")

    def store_task_metadata(self, metadata: TaskMetadata, schema: Schema):
        with self.__lock:
            self.run_metadata[schema.name][metadata.cache_key] = metadata

    def copy_task_metadata_to_working_schema(self, task: MaterialisingTask):
        schema = task.schema
        with self.__lock:
            self.run_metadata[schema.name][task.cache_key] = self.metadata[schema.name][task.cache_key]

    def retrieve_task_metadata(self, task: MaterialisingTask) -> TaskMetadata:
        with self.__lock:
            try:
                return self.metadata[task.schema.name][task.cache_key]
            except KeyError:
                raise CacheError(
                    f"Failed to retrieve metadata for task "
                    f"'{task.name}' with cache key '{task.cache_key}'")


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
            Column('run_id', String(20)),  # TODO: Replace with appropriate type
            Column('cache_key', String(20)),  # TODO: Replace with appropriate type
            Column('output_json', String),
            Column('in_working_schema', Boolean),

            schema = self.METADATA_SCHEMA,
        )

        self.lazy_cache_table = sa.Table(
            'lazy_tables', self.sql_metadata,

            Column('id', BigInteger, primary_key = True, autoincrement = True),
            Column('name', String),
            Column('schema', String),
            Column('cache_key', String(20)),
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

                conn.execute(
                    self.lazy_cache_table.delete()
                    .where(self.lazy_cache_table.c.schema == schema.name)
                    .where(self.lazy_cache_table.c.in_working_schema == False)
                )
                conn.execute(
                    self.lazy_cache_table.update()
                    .where(self.lazy_cache_table.c.schema == schema.name)
                    .values(in_working_schema = False)
                )

    def store_table(self, table: Table, lazy: bool):
        schema = table.schema
        if schema is None:
            raise ValueError(f"Table schema can't be None.")
        if schema.did_swap:
            raise SchemaError(f"Can't add new table to Schema '{schema.name}'. Schema has already been swapped.")
        if not isinstance(table.name, str):
            raise TypeError(f"Table name must be of instance 'str' not '{type(table.name).__name__}'.")

        obj = table.obj
        assert obj is not None

        logger = prefect.context.get('logger')

        if lazy:
            lazy_cache_key = self.compute_lazy_table_cache_key(table)
            lazy_table_md = self.retrieve_lazy_table_metadata(lazy_cache_key, schema.name)

            if lazy_table_md is not None:
                # Found in cache
                try:
                    self.copy_lazy_table_to_working_schema(lazy_table_md, table)
                    self.store_lazy_table_metadata(LazyTableMetadata(
                        name = table.name,
                        schema = schema.name,
                        cache_key = lazy_cache_key
                    ))
                    return
                except CacheError as e:
                    logger.warn(e)
        else:
            lazy_cache_key = None

        if isinstance(obj, pd.DataFrame):
            obj.to_sql(
                table.name,
                self.engine,
                schema = schema.working_name,
                if_exists = 'replace',
                index = False,
            )
        elif isinstance(obj, sa.sql.Select):
            # TODO: Handle table.primary_key
            logger.info(f'Performing CREATE TABLE AS SELECT ({table})')
            with self.engine.connect() as conn:
                conn.execute(CreateTableAsSelect(
                    table.name,
                    schema.working_name,
                    obj
                ))
        else:
            raise TypeError(f"Can't store Table with underlying type '{type(obj).__name__}'")

        if lazy and lazy_cache_key is not None:
            # Create Metadata
            self.store_lazy_table_metadata(LazyTableMetadata(
                name = table.name,
                schema = schema.name,
                cache_key = lazy_cache_key,
            ))

    def copy_table_to_working_schema(self, table: Table):
        schema = table.schema
        with self.engine.connect() as conn:
            if sa.inspect(self.engine).has_table(table.name, schema = schema.name):
                conn.execute(CopyTable(table.name, schema.name, table.name, schema.working_name))
            else:
                raise CacheError(
                    f"Can't copy table '{table.name}' (schema: '{schema.name}')"
                    f" to working schema because no such table exists.")

    def retrieve_table_obj(self, table: Table, as_type: Type[T], from_cache: bool = False) -> T:
        if as_type is None:
            raise TypeError(
                "Missing 'as_type' argument. You must specify a type to be able "
                "to dematerialise a Table.")

        schema = table.schema
        schema_name = schema.name if from_cache else schema.current_name

        if as_type == pd.DataFrame:
            with self.engine.connect() as conn:
                df = pd.read_sql_table(table.name, conn, schema = schema_name)
                return df

        if as_type == sa.Table:
            return sa.Table(
                table.name,
                sa.MetaData(bind = self.engine),
                schema = schema_name,
                autoload_with = self.engine,
            )

        raise TypeError(f"{type(self).__name__} can't convert to {as_type}.")

    def store_task_metadata(self, metadata: TaskMetadata, schema: Schema):
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
                .where(self.tasks_table.c.version == task.version)
                .where(self.tasks_table.c.cache_key == task.cache_key)
                .where(self.tasks_table.c.in_working_schema == False)
            ).mappings().one()

            metadata_copy = dict(metadata)
            metadata_copy['in_working_schema'] = True
            del metadata_copy['id']

            conn.execute(
                self.tasks_table.insert().values(**metadata_copy)
            )

    def retrieve_task_metadata(self, task: MaterialisingTask) -> TaskMetadata:
        with self.engine.connect() as conn:
            result = conn.execute(
                self.tasks_table.select()
                .where(self.tasks_table.c.schema == task.schema.name)
                .where(self.tasks_table.c.version == task.version)
                .where(self.tasks_table.c.cache_key == task.cache_key)
                .where(self.tasks_table.c.in_working_schema == False)
            ).mappings().one_or_none()

        if result is None:
            raise CacheError(f"Couldn't retrieve task for cache key {task.cache_key}")

        return TaskMetadata(
            name = result.name,
            schema = result.schema,
            version = result.version,
            timestamp = result.timestamp,
            run_id = result.run_id,
            cache_key = result.cache_key,
            output_json = result.output_json,
        )

    def compute_lazy_table_cache_key(self, table: Table) -> str | None:
        obj = table.obj
        v = [
            'PYDIVERSE-PIPEDAG-LAZY-TABLE',
            table.cache_key,  # Cache key of task
        ]

        if isinstance(obj, sa.sql.Select):
            query = str(obj.compile(self.engine, compile_kwargs = {'literal_binds': True}))
            v.append(query)
        else:
            return None

        v_str = '|'.join(v)
        v_bytes = v_str.encode('utf8')

        v_hash = hashlib.sha256(v_bytes)
        return v_hash.hexdigest()[:20]

    def store_lazy_table_metadata(self, metadata: LazyTableMetadata):
        with self.engine.connect() as conn:
            conn.execute(
                self.lazy_cache_table.insert().values(
                    name = metadata.name,
                    schema = metadata.schema,
                    cache_key = metadata.cache_key,
                    in_working_schema = True,
                )
            )

    def copy_lazy_table_to_working_schema(self, metadata: LazyTableMetadata, table: Table):
        with self.engine.connect() as conn:
            if sa.inspect(self.engine).has_table(metadata.name, schema = metadata.schema):
                conn.execute(CopyTable(metadata.name, metadata.schema, table.name, table.schema.working_name))
            else:
                raise CacheError(
                    f"Can't copy lazy table '{metadata.name}' (schema: '{metadata.schema}')"
                    f" to working schema because no such table exists.")

    def retrieve_lazy_table_metadata(self, cache_key: str, schema: str) -> LazyTableMetadata | None:
        if cache_key is None:
            return None

        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    self.lazy_cache_table.select()
                    .where(self.lazy_cache_table.c.schema == schema)
                    .where(self.lazy_cache_table.c.cache_key == cache_key)
                    .where(self.lazy_cache_table.c.in_working_schema == False)
                ).mappings().one_or_none()
        except sa.exc.MultipleResultsFound:
            return None

        if result is None:
            return None

        return LazyTableMetadata(
            name = result.name,
            schema = result.schema,
            cache_key = result.cache_key,
        )
