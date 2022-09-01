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

    def __init__(self, engine: sa.engine.Engine):
        super().__init__()

        self.engine = engine

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
            Column("run_id", String(20)),  # TODO: Replace with appropriate type
            Column("cache_key", String(20)),  # TODO: Replace with appropriate type
            Column("output_json", String),
            Column("in_transaction_schema", Boolean),
            schema=self.METADATA_SCHEMA,
        )

        self.lazy_cache_table = sa.Table(
            "lazy_tables",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("name", String),
            Column("stage", String),
            Column("cache_key", String(20)),
            Column("in_transaction_schema", Boolean),
            schema=self.METADATA_SCHEMA,
        )

    @classmethod
    def _init_conf_(cls, config: dict):
        engine_config = config.pop("engine")
        if isinstance(engine_config, str):
            engine_config = {"url": engine_config}

        engine_url = engine_config.pop("url")
        engine_config["_coerce_config"] = True
        engine = sa.create_engine(engine_url)

        return cls(engine=engine, **config)

    def setup(self):
        super().setup()
        with self.engine.connect() as conn:
            conn.execute(CreateSchema(self.METADATA_SCHEMA, if_not_exists=True))
            self.sql_metadata.create_all(conn)

    def close(self):
        self.engine.dispose()

    def init_stage(self, stage: Stage):
        cs_base = CreateSchema(stage.name, if_not_exists=True)
        ds_trans = DropSchema(stage.transaction_name, if_exists=True, cascade=True)
        cs_trans = CreateSchema(stage.transaction_name, if_not_exists=True)

        with self.engine.connect() as conn:
            conn.execute(cs_base)
            conn.execute(ds_trans)
            conn.execute(cs_trans)

            conn.execute(
                self.tasks_table.delete()
                .where(self.tasks_table.c.stage == stage.name)
                .where(self.tasks_table.c.in_transaction_schema == True)
            )

    def commit_stage(self, stage: Stage):
        tmp = stage.name + "__swap"
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(RenameSchema(stage.transaction_name, tmp))
                conn.execute(RenameSchema(stage.name, stage.transaction_name))
                conn.execute(RenameSchema(tmp, stage.name))
                conn.execute(DropSchema(tmp, if_exists=True, cascade=True))
                conn.execute(
                    DropSchema(stage.transaction_name, if_exists=True, cascade=True)
                )

                conn.execute(
                    self.tasks_table.delete()
                    .where(self.tasks_table.c.stage == stage.name)
                    .where(self.tasks_table.c.in_transaction_schema == False)
                )
                conn.execute(
                    self.tasks_table.update()
                    .where(self.tasks_table.c.stage == stage.name)
                    .values(in_transaction_schema=False)
                )

                conn.execute(
                    self.lazy_cache_table.delete()
                    .where(self.lazy_cache_table.c.stage == stage.name)
                    .where(self.lazy_cache_table.c.in_transaction_schema == False)
                )
                conn.execute(
                    self.lazy_cache_table.update()
                    .where(self.lazy_cache_table.c.stage == stage.name)
                    .values(in_transaction_schema=False)
                )

    def copy_table_to_transaction(self, table: Table):
        stage = table.stage
        if not sa.inspect(self.engine).has_table(table.name, schema=stage.name):
            raise CacheError(
                f"Can't copy table '{table.name}' (schema: '{stage.name}')"
                " to transaction because no such table exists."
            )

        with self.engine.connect() as conn:
            conn.execute(
                CopyTable(table.name, stage.name, table.name, stage.transaction_name)
            )

    def copy_lazy_table_to_transaction(self, metadata: LazyTableMetadata, table: Table):
        if not sa.inspect(self.engine).has_table(metadata.name, schema=metadata.stage):
            raise CacheError(
                f"Can't copy lazy table '{metadata.name}' (schema:"
                f" '{metadata.stage}') to transaction because no such table"
                " exists."
            )

        with self.engine.connect() as conn:
            conn.execute(
                CopyTable(
                    metadata.name,
                    metadata.stage,
                    table.name,
                    table.stage.transaction_name,
                )
            )

    def delete_table_from_transaction(self, table: Table):
        with self.engine.connect() as conn:
            conn.execute(
                DropTable(table.name, table.stage.transaction_name, if_exists=True)
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

    def copy_task_metadata_to_transaction(self, task: MaterializingTask):
        with self.engine.connect() as conn:
            metadata = (
                conn.execute(
                    self.tasks_table.select()
                    .where(self.tasks_table.c.stage == task.stage.name)
                    .where(self.tasks_table.c.version == task.version)
                    .where(self.tasks_table.c.cache_key == task.cache_key)
                    .where(self.tasks_table.c.in_transaction_schema == False)
                )
                .mappings()
                .one()
            )

            metadata_copy = dict(metadata)
            metadata_copy["in_transaction_schema"] = True
            del metadata_copy["id"]

            conn.execute(self.tasks_table.insert().values(**metadata_copy))

    def retrieve_task_metadata(self, task: MaterializingTask) -> TaskMetadata:
        try:
            with self.engine.connect() as conn:
                result = (
                    conn.execute(
                        self.tasks_table.select()
                        .where(self.tasks_table.c.stage == task.stage.name)
                        .where(self.tasks_table.c.version == task.version)
                        .where(self.tasks_table.c.cache_key == task.cache_key)
                        .where(self.tasks_table.c.in_transaction_schema == False)
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
        try:
            with self.engine.connect() as conn:
                result = (
                    conn.execute(
                        self.lazy_cache_table.select()
                        .where(self.lazy_cache_table.c.stage == stage.name)
                        .where(self.lazy_cache_table.c.cache_key == cache_key)
                        .where(self.lazy_cache_table.c.in_transaction_schema == False)
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
            conn.execute(CreateTableAsSelect(table.name, stage_name, table.obj))

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        return sa.Table(
            table.name,
            sa.MetaData(bind=store.engine),
            schema=stage_name,
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
            schema=stage_name,
            index=False,
        )

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        with store.engine.connect() as conn:
            df = pd.read_sql_table(table.name, conn, schema=stage_name)
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
