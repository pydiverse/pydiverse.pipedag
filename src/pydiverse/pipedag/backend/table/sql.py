from __future__ import annotations

import hashlib
import warnings

import pandas as pd
import prefect
import sqlalchemy as sa

from pydiverse.pipedag._typing import T
from pydiverse.pipedag.backend.metadata import LazyTableMetadata, TaskMetadata
from pydiverse.pipedag.backend.table.base import BaseTableStore, TableHook
from pydiverse.pipedag.backend.table.util.sql_ddl import (
    CopyTable,
    CreateSchema,
    CreateTableAsSelect,
    DropSchema,
    RenameSchema,
)
from pydiverse.pipedag.core import MaterialisingTask, Schema, Table
from pydiverse.pipedag.errors import CacheError, SchemaError


class SQLTableStore(BaseTableStore):
    """Table store that materialises tables to a SQL database"""

    METADATA_SCHEMA = "pipedag_metadata"

    def __init__(self, engine: sa.engine.Engine):
        self.engine = engine

        # Set up metadata tables and schema
        from sqlalchemy import BigInteger, Boolean, Column, DateTime, String

        self.sql_metadata = sa.MetaData()
        self.tasks_table = sa.Table(
            "tasks",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("name", String),
            Column("schema", String),
            Column("version", String),
            Column("timestamp", DateTime),
            Column("run_id", String(20)),  # TODO: Replace with appropriate type
            Column("cache_key", String(20)),  # TODO: Replace with appropriate type
            Column("output_json", String),
            Column("in_working_schema", Boolean),
            schema=self.METADATA_SCHEMA,
        )

        self.lazy_cache_table = sa.Table(
            "lazy_tables",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("name", String),
            Column("schema", String),
            Column("cache_key", String(20)),
            Column("in_working_schema", Boolean),
            schema=self.METADATA_SCHEMA,
        )

    def setup(self):
        super().setup()
        with self.engine.connect() as conn:
            conn.execute(CreateSchema(self.METADATA_SCHEMA, if_not_exists=True))
            self.sql_metadata.create_all(conn)

    def create_schema(self, schema: Schema):
        cs_main = CreateSchema(schema.name, if_not_exists=True)
        ds_working = DropSchema(schema.working_name, if_exists=True, cascade=True)
        cs_working = CreateSchema(schema.working_name, if_not_exists=True)

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
        tmp = schema.name + "__tmp_swap"
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(RenameSchema(schema.working_name, tmp))
                conn.execute(RenameSchema(schema.name, schema.working_name))
                conn.execute(RenameSchema(tmp, schema.name))
                conn.execute(DropSchema(tmp, if_exists=True, cascade=True))
                conn.execute(
                    DropSchema(schema.working_name, if_exists=True, cascade=True)
                )

                conn.execute(
                    self.tasks_table.delete()
                    .where(self.tasks_table.c.schema == schema.name)
                    .where(self.tasks_table.c.in_working_schema == False)
                )
                conn.execute(
                    self.tasks_table.update()
                    .where(self.tasks_table.c.schema == schema.name)
                    .values(in_working_schema=False)
                )

                conn.execute(
                    self.lazy_cache_table.delete()
                    .where(self.lazy_cache_table.c.schema == schema.name)
                    .where(self.lazy_cache_table.c.in_working_schema == False)
                )
                conn.execute(
                    self.lazy_cache_table.update()
                    .where(self.lazy_cache_table.c.schema == schema.name)
                    .values(in_working_schema=False)
                )

    def store_table(self, table: Table, lazy: bool):
        schema = table.schema
        if schema is None:
            raise ValueError(f"Table schema can't be None.")
        if schema.did_swap:
            raise SchemaError(
                f"Can't add new table to Schema '{schema.name}'. Schema has already"
                " been swapped."
            )
        if not isinstance(table.name, str):
            raise TypeError(
                "Table name must be of instance 'str' not"
                f" '{type(table.name).__name__}'."
            )
        if table.obj is None:
            raise TypeError("Table object can't be None.")

        if lazy:
            lazy_cache_key = self.compute_lazy_table_cache_key(table)
            lazy_table_md = self.retrieve_lazy_table_metadata(
                lazy_cache_key, schema.name
            )

            if lazy_table_md is not None:
                # Found in cache
                try:
                    self.copy_lazy_table_to_working_schema(lazy_table_md, table)
                    self.store_lazy_table_metadata(
                        LazyTableMetadata(
                            name=table.name,
                            schema=schema.name,
                            cache_key=lazy_cache_key,
                        )
                    )
                    return
                except CacheError as e:
                    prefect.context.logger.warn(e)
        else:
            lazy_cache_key = None

        hook = self.get_m_table_hook(type(table.obj))
        hook.materialise(self, table, schema.working_name)

        if lazy and lazy_cache_key is not None:
            # Create Metadata
            self.store_lazy_table_metadata(
                LazyTableMetadata(
                    name=table.name,
                    schema=schema.name,
                    cache_key=lazy_cache_key,
                )
            )

    def copy_table_to_working_schema(self, table: Table):
        schema = table.schema
        with self.engine.connect() as conn:
            if sa.inspect(self.engine).has_table(table.name, schema=schema.name):
                conn.execute(
                    CopyTable(table.name, schema.name, table.name, schema.working_name)
                )
            else:
                raise CacheError(
                    f"Can't copy table '{table.name}' (schema: '{schema.name}')"
                    " to working schema because no such table exists."
                )

    def retrieve_table_obj(
        self, table: Table, as_type: type[T], from_cache: bool = False
    ) -> T:
        if as_type is None:
            raise TypeError(
                "Missing 'as_type' argument. You must specify a type to be able "
                "to dematerialise a Table."
            )

        schema = table.schema
        schema_name = schema.name if from_cache else schema.current_name

        hook = self.get_r_table_hook(as_type)
        return hook.retrieve(self, table, schema_name, as_type)

    def store_task_metadata(self, metadata: TaskMetadata, schema: Schema):
        with self.engine.connect() as conn:
            conn.execute(
                self.tasks_table.insert().values(
                    name=metadata.name,
                    schema=metadata.schema,
                    version=metadata.version,
                    timestamp=metadata.timestamp,
                    run_id=metadata.run_id,
                    cache_key=metadata.cache_key,
                    output_json=metadata.output_json,
                    in_working_schema=True,
                )
            )

    def copy_task_metadata_to_working_schema(self, task: MaterialisingTask):
        with self.engine.connect() as conn:
            metadata = (
                conn.execute(
                    self.tasks_table.select()
                    .where(self.tasks_table.c.schema == task.schema.name)
                    .where(self.tasks_table.c.version == task.version)
                    .where(self.tasks_table.c.cache_key == task.cache_key)
                    .where(self.tasks_table.c.in_working_schema == False)
                )
                .mappings()
                .one()
            )

            metadata_copy = dict(metadata)
            metadata_copy["in_working_schema"] = True
            del metadata_copy["id"]

            conn.execute(self.tasks_table.insert().values(**metadata_copy))

    def retrieve_task_metadata(self, task: MaterialisingTask) -> TaskMetadata:
        with self.engine.connect() as conn:
            result = (
                conn.execute(
                    self.tasks_table.select()
                    .where(self.tasks_table.c.schema == task.schema.name)
                    .where(self.tasks_table.c.version == task.version)
                    .where(self.tasks_table.c.cache_key == task.cache_key)
                    .where(self.tasks_table.c.in_working_schema == False)
                )
                .mappings()
                .one_or_none()
            )

        if result is None:
            raise CacheError(f"Couldn't retrieve task for cache key {task.cache_key}")

        return TaskMetadata(
            name=result.name,
            schema=result.schema,
            version=result.version,
            timestamp=result.timestamp,
            run_id=result.run_id,
            cache_key=result.cache_key,
            output_json=result.output_json,
        )

    #### Lazy Table Implementation ####

    def compute_lazy_table_cache_key(self, table: Table) -> str | None:
        obj = table.obj
        v = [
            "PYDIVERSE-PIPEDAG-LAZY-TABLE",
            table.cache_key,  # Cache key of task
        ]

        if isinstance(obj, sa.sql.Select):
            query = str(
                obj.compile(self.engine, compile_kwargs={"literal_binds": True})
            )
            v.append(query)
        else:
            return None

        v_str = "|".join(v)
        v_bytes = v_str.encode("utf8")

        v_hash = hashlib.sha256(v_bytes)
        return v_hash.hexdigest()[:20]

    def store_lazy_table_metadata(self, metadata: LazyTableMetadata):
        with self.engine.connect() as conn:
            conn.execute(
                self.lazy_cache_table.insert().values(
                    name=metadata.name,
                    schema=metadata.schema,
                    cache_key=metadata.cache_key,
                    in_working_schema=True,
                )
            )

    def copy_lazy_table_to_working_schema(
        self, metadata: LazyTableMetadata, table: Table
    ):
        with self.engine.connect() as conn:
            if sa.inspect(self.engine).has_table(metadata.name, schema=metadata.schema):
                conn.execute(
                    CopyTable(
                        metadata.name,
                        metadata.schema,
                        table.name,
                        table.schema.working_name,
                    )
                )
            else:
                raise CacheError(
                    f"Can't copy lazy table '{metadata.name}' (schema:"
                    f" '{metadata.schema}') to working schema because no such table"
                    " exists."
                )

    def retrieve_lazy_table_metadata(
        self, cache_key: str, schema: str
    ) -> LazyTableMetadata | None:
        if cache_key is None:
            return None

        try:
            with self.engine.connect() as conn:
                result = (
                    conn.execute(
                        self.lazy_cache_table.select()
                        .where(self.lazy_cache_table.c.schema == schema)
                        .where(self.lazy_cache_table.c.cache_key == cache_key)
                        .where(self.lazy_cache_table.c.in_working_schema == False)
                    )
                    .mappings()
                    .one_or_none()
                )
        except sa.exc.MultipleResultsFound:
            return None

        if result is None:
            return None

        return LazyTableMetadata(
            name=result.name,
            schema=result.schema,
            cache_key=result.cache_key,
        )


@SQLTableStore.register_table()
class SQLAlchemyTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialise(cls, type_) -> bool:
        return issubclass(type_, sa.sql.Select)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == sa.Table

    @classmethod
    def materialise(cls, store, table: Table[sa.sql.Select], schema_name):
        prefect.context.logger.info(f"Performing CREATE TABLE AS SELECT ({table})")
        with store.engine.connect() as conn:
            conn.execute(CreateTableAsSelect(table.name, schema_name, table.obj))

    @classmethod
    def retrieve(cls, store, table, schema_name, as_type):
        return sa.Table(
            table.name,
            sa.MetaData(bind=store.engine),
            schema=schema_name,
            autoload_with=store.engine,
        )


@SQLTableStore.register_table(pd)
class PandasTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialise(cls, type_) -> bool:
        return issubclass(type_, pd.DataFrame)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == pd.DataFrame

    @classmethod
    def materialise(cls, store, table: Table[pd.DataFrame], schema_name):
        table.obj.to_sql(
            table.name,
            store.engine,
            schema=schema_name,
            index=False,
        )

    @classmethod
    def retrieve(cls, store, table, schema_name, as_type):
        with store.engine.connect() as conn:
            df = pd.read_sql_table(table.name, conn, schema=schema_name)
            return df


try:
    import pydiverse.transform as pdt
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    pdt = None


@SQLTableStore.register_table(pdt)
class PydiverseTransformTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialise(cls, type_) -> bool:
        return issubclass(type_, pdt.Table)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        return issubclass(type_, (PandasTableImpl, SQLTableImpl))

    @classmethod
    def materialise(cls, store, table: Table[pdt.Table], schema_name):
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        t = table.obj
        if isinstance(t._impl, PandasTableImpl):
            from pydiverse.transform.core.verbs import collect

            table.obj = t >> collect()
            return PandasTableHook.materialise(store, table, schema_name)
        if isinstance(t._impl, SQLTableImpl):
            table.obj = t._impl.build_select()
            return SQLAlchemyTableHook.materialise(store, table, schema_name)
        raise NotImplementedError

    @classmethod
    def retrieve(cls, store, table, schema_name, as_type):
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        if issubclass(as_type, PandasTableImpl):
            df = PandasTableHook.retrieve(store, table, schema_name, pd.DataFrame)
            return pdt.Table(PandasTableImpl(table.name, df))
        if issubclass(as_type, SQLTableImpl):
            sa_tbl = SQLAlchemyTableHook.retrieve(store, table, schema_name, sa.Table)
            return pdt.Table(SQLTableImpl(store.engine, sa_tbl))
        raise NotImplementedError
