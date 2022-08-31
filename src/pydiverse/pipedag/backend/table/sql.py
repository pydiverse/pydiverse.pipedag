from __future__ import annotations

import warnings
from typing import Union

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.sql.elements

from pydiverse.pipedag import Stage, Table
from pydiverse.pipedag.backend.table.base import BaseTableStore, TableHook
from pydiverse.pipedag.backend.table.util.sql_ddl import (
    CopyTable,
    CreateSchema,
    CreateTableAsSelect,
    DropSchema,
    DropTable,
    RenameSchema,
    Schema,
)
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.core import MaterializingTask
from pydiverse.pipedag.materialize.metadata import LazyTableMetadata, TaskMetadata


class SQLTableStore(BaseTableStore):
    """Table store that materializes tables to a SQL database

    Uses schema swapping for transactions:
    Creates a schema for each stage and a temporary schema for each
    transaction. If all tasks inside a stage succeed, swaps the schemas by
    renaming them.
    """

    METADATA_SCHEMA = "pipedag_metadata"

    def __init__(
        self, engine: sa.engine.Engine, schema_prefix: str = "", schema_suffix: str = ""
    ):
        super().__init__()

        self.engine = engine
        self.schema_prefix = schema_prefix
        self.schema_suffix = schema_suffix
        self.metadata_schema = self.get_schema(self.METADATA_SCHEMA)

        if engine.dialect.name == "mssql":
            if "." in schema_prefix and "." in schema_suffix:
                raise AttributeError(
                    "Config Error: It is not allowed to have a dot in both"
                    " schema_prefix and schema_suffix for SQL Server / mssql database:"
                    f' schema_prefix="{schema_prefix}", schema_suffix="{schema_suffix}"'
                )

            if (schema_prefix + schema_suffix).count(".") != 1:
                raise AttributeError(
                    "Config Error: There must be exactly dot in both schema_prefix and"
                    " schema_suffix together for SQL Server / mssql database:"
                    f' schema_prefix="{schema_prefix}", schema_suffix="{schema_suffix}"'
                )

        # Set up metadata tables and schema
        from sqlalchemy import BigInteger, Boolean, Column, DateTime, String

        self.sql_metadata = sa.MetaData()
        self.tasks_table = sa.Table(
            "tasks",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("name", String),
            Column("stage", String),
            Column("version", String),
            Column("timestamp", DateTime),
            Column("run_id", String(32)),  # TODO: Replace with appropriate type
            Column("cache_key", String(64)),  # TODO: Replace with appropriate type
            Column("output_json", String),
            Column("in_transaction_schema", Boolean),
            schema=self.metadata_schema.get(),
        )

        self.lazy_cache_table = sa.Table(
            "lazy_tables",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("name", String),
            Column("stage", String),
            Column("cache_key", String(64)),
            Column("in_transaction_schema", Boolean),
            schema=self.metadata_schema.get(),
        )

    @classmethod
    def _init_conf_(cls, config: dict):
        engine_config = config.pop("engine")
        if isinstance(engine_config, str):
            engine_config = {"url": engine_config}

        engine_url = engine_config.pop("url")
        engine_config["_coerce_config"] = True
        engine = sa.create_engine(engine_url)
        if engine.dialect.name == "mssql":
            engine.dispose()
            # this is needed to allow for CREATE DATABASE statements (we don't rely on database transactions anyways)
            engine = sa.create_engine(engine_url, connect_args={"autocommit": True})

        return cls(engine=engine, **config)

    def get_schema(self, name):
        return Schema(name, self.schema_prefix, self.schema_suffix)

    def setup(self):
        super().setup()
        with self.engine.connect() as conn:
            conn.execute(CreateSchema(self.metadata_schema, if_not_exists=True))
        with self.engine.connect() as conn:
            self.sql_metadata.create_all(conn)

    def close(self):
        self.engine.dispose()

    def init_stage(self, stage: Stage):
        cs_base = CreateSchema(self.get_schema(stage.name), if_not_exists=True)
        ds_trans = DropSchema(
            self.get_schema(stage.transaction_name), if_exists=True, cascade=True
        )
        cs_trans = CreateSchema(
            self.get_schema(stage.transaction_name), if_not_exists=False
        )

        if self.engine.dialect.name == "mssql" and "." in self.schema_suffix:
            # TODO: detect whether tmp_schema exists and rename it as base or transaction schema if one is missing
            # tmp_schema = self.get_schema(stage.name + "__swap")
            cs_trans_initial = CreateSchema(
                self.get_schema(stage.transaction_name), if_not_exists=True
            )
            full_name = self.get_schema(stage.transaction_name).get()
            database, schema = full_name.split(".")

            # don't drop/create databases, just replace the schema underneath (files will keep name on renaming)
            with self.engine.connect() as conn:
                conn.execute(cs_base)
                conn.execute(cs_trans_initial)
                conn.execute(f"USE [{database}]")
                # clear tables in schema
                sql = f"""
                    EXEC sp_MSforeachtable
                      @command1 = 'DROP TABLE ?'
                    , @whereand = 'AND SCHEMA_NAME(schema_id) = ''{schema}'' '
                """
                conn.execute(sql)

                conn.execute(
                    self.tasks_table.delete()
                    .where(self.tasks_table.c.stage == stage.name)
                    .where(self.tasks_table.c.in_transaction_schema.in_([True]))
                )
        else:
            with self.engine.connect() as conn:
                conn.execute(cs_base)
                conn.execute(ds_trans)
                conn.execute(cs_trans)

                conn.execute(
                    self.tasks_table.delete()
                    .where(self.tasks_table.c.stage == stage.name)
                    .where(self.tasks_table.c.in_transaction_schema.in_([True]))
                )

    def commit_stage(self, stage: Stage):
        tmp_schema = self.get_schema(stage.name + "__swap")
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(DropSchema(tmp_schema, if_exists=True, cascade=True))
                # TODO: in case "." is in self.schema_prefix, we need to implement schema renaming by
                #  creating the new schema and moving table objects over
                conn.execute(
                    RenameSchema(
                        self.get_schema(stage.name),
                        tmp_schema,
                    )
                )
                conn.execute(
                    RenameSchema(
                        self.get_schema(stage.transaction_name),
                        self.get_schema(stage.name),
                    )
                )
                conn.execute(
                    RenameSchema(tmp_schema, self.get_schema(stage.transaction_name))
                )

                for table in [self.tasks_table, self.lazy_cache_table]:
                    conn.execute(
                        table.delete()
                        .where(table.c.stage == stage.name)
                        .where(table.c.in_transaction_schema.in_([False]))
                    )
                    conn.execute(
                        table.update()
                        .where(table.c.stage == stage.name)
                        .values(in_transaction_schema=False)
                    )

    def copy_table_to_transaction(self, table: Table):
        stage = table.stage
        if not sa.inspect(self.engine).has_table(
            table.name, schema=self.get_schema(stage.name).get()
        ):
            raise CacheError(
                f"Can't copy table '{table.name}' (schema: '{stage.name}')"
                " to transaction because no such table exists."
            )

        with self.engine.connect() as conn:
            conn.execute(
                CopyTable(
                    table.name,
                    self.get_schema(stage.name),
                    table.name,
                    self.get_schema(stage.transaction_name),
                )
            )

    def copy_lazy_table_to_transaction(self, metadata: LazyTableMetadata, table: Table):
        if not sa.inspect(self.engine).has_table(
            metadata.name, schema=self.get_schema(metadata.stage).get()
        ):
            raise CacheError(
                f"Can't copy lazy table '{metadata.name}' (schema:"
                f" '{metadata.stage}') to transaction because no such table"
                " exists."
            )

        with self.engine.connect() as conn:
            conn.execute(
                CopyTable(
                    metadata.name,
                    self.get_schema(metadata.stage),
                    table.name,
                    self.get_schema(table.stage.transaction_name),
                )
            )

    def delete_table_from_transaction(self, table: Table):
        with self.engine.connect() as conn:
            conn.execute(
                DropTable(
                    table.name,
                    self.get_schema(table.stage.transaction_name),
                    if_exists=True,
                )
            )

    def store_task_metadata(self, metadata: TaskMetadata, stage: Stage):
        with self.engine.connect() as conn:
            conn.execute(
                self.tasks_table.insert().values(
                    name=metadata.name,
                    stage=metadata.stage,
                    version=metadata.version,
                    timestamp=metadata.timestamp,
                    run_id=metadata.run_id,
                    cache_key=metadata.cache_key,
                    output_json=metadata.output_json,
                    in_transaction_schema=True,
                )
            )

    # noinspection DuplicatedCode
    def copy_task_metadata_to_transaction(self, task: MaterializingTask):
        with self.engine.connect() as conn:
            metadata = (
                conn.execute(
                    self.tasks_table.select()
                    .where(self.tasks_table.c.stage == task.stage.name)
                    .where(self.tasks_table.c.version == task.version)
                    .where(self.tasks_table.c.cache_key == task.cache_key)
                    .where(self.tasks_table.c.in_transaction_schema.in_([False]))
                )
                .mappings()
                .one()
            )

            metadata_copy = dict(metadata)
            metadata_copy["in_transaction_schema"] = True
            del metadata_copy["id"]

            conn.execute(self.tasks_table.insert().values(**metadata_copy))

    # noinspection DuplicatedCode
    def retrieve_task_metadata(self, task: MaterializingTask) -> TaskMetadata:
        # noinspection PyUnresolvedReferences
        try:
            with self.engine.connect() as conn:
                result = (
                    conn.execute(
                        self.tasks_table.select()
                        .where(self.tasks_table.c.stage == task.stage.name)
                        .where(self.tasks_table.c.version == task.version)
                        .where(self.tasks_table.c.cache_key == task.cache_key)
                        .where(self.tasks_table.c.in_transaction_schema.in_([False]))
                    )
                    .mappings()
                    .one_or_none()
                )
        except sa.exc.MultipleResultsFound:
            raise CacheError("Multiple results found task metadata")

        if result is None:
            raise CacheError(f"Couldn't retrieve task for cache key {task.cache_key}")

        return TaskMetadata(
            name=result.name,
            stage=result.stage,
            version=result.version,
            timestamp=result.timestamp,
            run_id=result.run_id,
            cache_key=result.cache_key,
            output_json=result.output_json,
        )

    def store_lazy_table_metadata(self, metadata: LazyTableMetadata):
        with self.engine.connect() as conn:
            conn.execute(
                self.lazy_cache_table.insert().values(
                    name=metadata.name,
                    stage=metadata.stage,
                    cache_key=metadata.cache_key,
                    in_transaction_schema=True,
                )
            )

    def retrieve_lazy_table_metadata(
        self, cache_key: str, stage: Stage
    ) -> LazyTableMetadata:
        # noinspection PyUnresolvedReferences
        try:
            with self.engine.connect() as conn:
                result = (
                    conn.execute(
                        self.lazy_cache_table.select()
                        .where(self.lazy_cache_table.c.stage == stage.name)
                        .where(self.lazy_cache_table.c.cache_key == cache_key)
                        .where(
                            self.lazy_cache_table.c.in_transaction_schema.in_([False])
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except sa.exc.MultipleResultsFound:
            raise CacheError("Multiple results found for lazy table cache key")

        if result is None:
            raise CacheError("No result found for lazy table cache key")

        return LazyTableMetadata(
            name=result.name,
            stage=result.stage,
            cache_key=result.cache_key,
        )


@SQLTableStore.register_table()
class SQLAlchemyTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, (sa.sql.Select, sa.sql.elements.TextClause))

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == sa.Table

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[sa.sql.elements.TextClause | sa.Text],
        stage_name,
    ):
        store.logger.info(f"Performing CREATE TABLE AS SELECT ({table})")
        with store.engine.connect() as conn:
            conn.execute(
                CreateTableAsSelect(
                    table.name, store.get_schema(stage_name).get(), table.obj
                )
            )

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        return sa.Table(
            table.name,
            sa.MetaData(bind=store.engine),
            schema=store.get_schema(stage_name).get(),
            autoload_with=store.engine,
        )

    @classmethod
    def lazy_query_str(cls, store, obj) -> str:
        return str(obj.compile(store.engine, compile_kwargs={"literal_binds": True}))


@SQLTableStore.register_table(pd)
class PandasTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pd.DataFrame)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == pd.DataFrame

    @classmethod
    def materialize(cls, store, table: Table[pd.DataFrame], stage_name):
        table.obj.to_sql(
            table.name,
            store.engine,
            schema=store.get_schema(stage_name).get(),
            index=False,
        )

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        with store.engine.connect() as conn:
            df = pd.read_sql_table(
                table.name, conn, schema=store.get_schema(stage_name).get()
            )
            return df

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


# noinspection PyUnresolvedReferences,PyProtectedMember
@SQLTableStore.register_table(pdt)
class PydiverseTransformTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pdt.Table)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        return issubclass(type_, (PandasTableImpl, SQLTableImpl))

    @classmethod
    def materialize(cls, store, table: Table[pdt.Table], stage_name):
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        t = table.obj
        if isinstance(t._impl, PandasTableImpl):
            from pydiverse.transform.core.verbs import collect

            table.obj = t >> collect()
            return PandasTableHook.materialize(store, table, stage_name)
        if isinstance(t._impl, SQLTableImpl):
            table.obj = t._impl.build_select()
            return SQLAlchemyTableHook.materialize(store, table, stage_name)
        raise NotImplementedError

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        if issubclass(as_type, PandasTableImpl):
            df = PandasTableHook.retrieve(store, table, stage_name, pd.DataFrame)
            return pdt.Table(PandasTableImpl(table.name, df))
        if issubclass(as_type, SQLTableImpl):
            sa_tbl = SQLAlchemyTableHook.retrieve(store, table, stage_name, sa.Table)
            return pdt.Table(SQLTableImpl(store.engine, sa_tbl))
        raise NotImplementedError

    @classmethod
    def auto_table(cls, obj: pdt.Table):
        return Table(obj, obj._impl.name)

    @classmethod
    def lazy_query_str(cls, store, obj: pdt.Table) -> str:
        from pydiverse.transform.core.verbs import build_query

        query = obj >> build_query()

        if query is not None:
            return str(query)
        return super().lazy_query_str(store, obj)
