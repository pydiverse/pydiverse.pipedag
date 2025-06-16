# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import re
import time
import types
import typing
import warnings
from typing import Any

import sqlalchemy as sa
import sqlalchemy.exc
import structlog
from packaging.version import Version

from pydiverse.common import Date, Dtype, PandasBackend
from pydiverse.common.util.computation_tracing import ComputationTracer
from pydiverse.pipedag import ConfigContext
from pydiverse.pipedag._typing import T
from pydiverse.pipedag.backend.table.sql.ddl import (
    CreateTableAsSelect,
    InsertIntoSelect,
)
from pydiverse.pipedag.backend.table.sql.sql import (
    SQLTableStore,
)
from pydiverse.pipedag.container import ExternalTableReference, Schema, Table
from pydiverse.pipedag.context import TaskContext
from pydiverse.pipedag.materialize.details import resolve_materialization_details_label
from pydiverse.pipedag.materialize.table_hook_base import (
    AutoVersionSupport,
    CanResult,
    TableHook,
)

try:
    import polars as pl
except ImportError:
    pl = None

# region SQLALCHEMY


def _polars_apply_retrieve_annotation(df, table, intentionally_empty: bool = False):
    try:
        import dataframely as dy

        if isinstance(table.annotation, typing.GenericAlias) and issubclass(
            typing.get_origin(table.annotation), dy.LazyFrame | dy.DataFrame
        ):
            anno_args = typing.get_args(table.annotation)
            if len(anno_args) == 1:
                column_spec = anno_args[0]
                if issubclass(column_spec, dy.Schema | dy.Collection):
                    df = column_spec.cast(df)
    except ImportError:
        # If dataframely is not installed, we can't apply the annotation.
        pass
    return df


def _polars_apply_materialize_annotation(df, table):
    try:
        import dataframely as dy

        if isinstance(table.annotation, typing.GenericAlias) and issubclass(
            typing.get_origin(table.annotation), dy.LazyFrame | dy.DataFrame
        ):
            anno_args = typing.get_args(table.annotation)
            if len(anno_args) == 1:
                column_spec = anno_args[0]
                if issubclass(column_spec, dy.Schema | dy.Collection):
                    df = column_spec.validate(df, cast=True)
    except ImportError:
        # If dataframely is not installed, we can't apply the annotation.
        pass
    return df


@SQLTableStore.register_table()
class SQLAlchemyTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanResult:
        type_ = type(tbl.obj)
        return CanResult.new(
            issubclass(
                type_, (sa.sql.expression.TextClause, sa.sql.expression.Selectable)
            )
        )

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == sa.Table

    @classmethod
    def retrieve_as_reference(cls, type_):
        return True

    @classmethod
    def materialize(
        cls,
        store: SQLTableStore,
        table: Table[sa.sql.expression.TextClause | sa.sql.expression.Selectable],
        stage_name,
    ):
        query = table.obj
        if isinstance(table.obj, (sa.Table, sa.sql.expression.Alias)):
            query = sa.select("*").select_from(table.obj)
            tbl = table.obj if isinstance(table.obj, sa.Table) else table.obj.original
            source_tables = [
                dict(
                    name=tbl.name,
                    schema=tbl.schema,
                    shared_lock_allowed=table.shared_lock_allowed,
                )
            ]
        else:
            try:
                input_tables = TaskContext.get().input_tables
            except LookupError:
                input_tables = []
            source_tables = [
                dict(
                    name=tbl.name,
                    schema=store.get_schema(tbl.stage.current_name).get()
                    if tbl.external_schema is None
                    else tbl.external_schema,
                    shared_lock_allowed=tbl.shared_lock_allowed,
                )
                for tbl in input_tables
            ]

        schema = store.get_schema(stage_name)

        store.check_materialization_details_supported(
            resolve_materialization_details_label(table)
        )

        suffix = store.get_create_table_suffix(
            resolve_materialization_details_label(table)
        )
        unlogged = store.get_unlogged(resolve_materialization_details_label(table))
        if store.dialect_requests_empty_creation(table, is_sql=True):
            cls._create_table_as_select_empty_insert(
                table, schema, query, source_tables, store, suffix, unlogged
            )
        else:
            cls._create_table_as_select(
                table, schema, query, source_tables, store, suffix, unlogged
            )
        store.optional_pause_for_db_transactionality("table_create")

    @classmethod
    def _create_table_as_select(
        cls, table, schema, query, source_tables, store, suffix, unlogged
    ):
        statements = store.lock_source_tables(source_tables)
        statements += cls._create_as_select_statements(
            table.name, schema, query, store, suffix, unlogged
        )
        store.execute(statements)
        store.add_indexes_and_set_nullable(table, schema)

    @classmethod
    def _create_table_as_select_empty_insert(
        cls, table, schema, query, source_tables, store, suffix, unlogged
    ):
        limit_query = store.get_limit_query(query, rows=0)
        store.execute(
            cls._create_as_select_statements(
                table.name, schema, limit_query, store, suffix, unlogged
            )
        )
        store.add_indexes_and_set_nullable(table, schema, on_empty_table=True)
        statements = store.lock_table(table, schema)
        statements += store.lock_source_tables(source_tables)
        statements += cls._insert_as_select_statements(table.name, schema, query, store)
        store.execute(
            statements,
            truncate_printed_select=True,
        )
        store.add_indexes_and_set_nullable(table, schema, on_empty_table=False)

    @classmethod
    def _create_as_select_statements(
        cls,
        table_name: str,
        schema: Schema,
        query: sa.Select | sa.TextClause | sa.Text,
        store: SQLTableStore,
        suffix: str,
        unlogged: bool,
    ):
        _ = store
        return [
            CreateTableAsSelect(
                table_name,
                schema,
                query,
                unlogged=unlogged,
                suffix=suffix,
            )
        ]

    @classmethod
    def _insert_as_select_statements(
        cls,
        table_name: str,
        schema: Schema,
        query: sa.Select | sa.TextClause | sa.Text,
        store: SQLTableStore,
    ):
        _ = store
        return [
            InsertIntoSelect(
                table_name,
                schema,
                query,
            )
        ]

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[sa.Table],
    ) -> sa.sql.expression.Selectable:
        table_name, schema = store.resolve_alias(table, stage_name)
        try:
            alias_name = TaskContext.get().name_disambiguator.get_name(table_name)
        except LookupError:
            # Used for imperative materialization with explicit config_context
            alias_name = table_name

        tbl = store.reflect_table(table_name, schema)
        return tbl.alias(alias_name)

    @classmethod
    def lazy_query_str(cls, store, obj) -> str:
        if isinstance(obj, sa.sql.expression.FromClause):
            query = sa.select("*").select_from(obj)
        else:
            query = obj
        query_str = str(
            query.compile(store.engine, compile_kwargs={"literal_binds": True})
        )
        # hacky way to canonicalize query (despite __tmp/__even/__odd suffixes
        # and alias resolution)
        query_str = re.sub(r'["\[\]]', "", query_str)
        query_str = re.sub(
            r'(__tmp|__even|__odd)(?=[ \t\n.;"]|$)', "", query_str.lower()
        )
        return query_str


@SQLTableStore.register_table()
class ExternalTableReferenceHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanResult:
        type_ = type(tbl.obj)
        return CanResult.new(issubclass(type_, ExternalTableReference))

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return False

    @classmethod
    def materialize(cls, store: SQLTableStore, table: Table, stage_name: str):
        # For an external table reference, we don't need to materialize anything.
        # This is any table referenced by a table reference should already exist
        # in the schema.
        # Instead, we check that the table actually exists.
        stage_schema = store.get_schema(stage_name).get()
        if table.external_schema.upper() == stage_schema.upper():
            raise ValueError(
                f"ExternalTableReference '{table.name}' is not allowed to reference "
                f"tables in the transaction schema '{stage_schema}' of the current "
                "stage."
            )
        if stage_schema.upper().startswith(table.external_schema.upper() + "__"):
            raise ValueError(
                f"ExternalTableReference '{table.name}' is not allowed to reference "
                f"tables in the schema '{table.external_schema}' of the current stage."
            )

        has_table = store.has_table_or_view(table.name, table.external_schema)

        if not has_table:
            raise ValueError(
                f"No table with name '{table.name}' found in schema "
                f"'{table.external_schema}' (reference by ExternalTableReference)."
            )

        return

    @classmethod
    def retrieve(cls, *args, **kwargs):
        raise RuntimeError("This should never get called.")


# endregion

# region PANDAS
try:
    import pandas as pd
except ImportError:
    pd = None


@SQLTableStore.register_table(pd)
class PandasTableHook(TableHook[SQLTableStore]):
    """
    Allows overriding the default dtype backend to use by setting the `dtype_backend`
    argument in the `hook_args` section of the table store config::

        hook_args:
          pandas:
            dtype_backend: "arrow" | "numpy"
    """

    pd_version = Version(pd.__version__)
    auto_version_support = AutoVersionSupport.TRACE

    @classmethod
    def upload_table(
        cls,
        df: pd.DataFrame,
        name: str,
        schema: str,
        dtypes: dict[str, Dtype],
        conn: sa.Connection,
        early: bool,
    ):
        """
        Provide hook that allows to override the default
        upload of pandas/polars tables to the tablestore.
        """
        df.to_sql(
            name,
            conn,
            schema=schema,
            index=False,
            dtype=dtypes,
            chunksize=100_000,
            if_exists="append" if early else "fail",
        )

    @classmethod
    def download_table(
        cls, query: Any, conn: sa.Connection, dtypes: dict[str, Dtype] | None = None
    ) -> pd.DataFrame:
        """
        Provide hook that allows to override the default
        download of pandas tables from the tablestore.
        Also serves as fallback for polars download.
        """
        if PandasTableHook.pd_version >= Version("2.0"):
            df = pd.read_sql(query, con=conn, dtype=dtypes)
        else:
            df = pd.read_sql(query, con=conn)
            if dtypes is not None:
                for col, dtype in dtypes.items():
                    df[col] = df[col].astype(dtype)
        return df

    @classmethod
    def can_materialize(cls, tbl: Table) -> CanResult:
        type_ = type(tbl.obj)
        return CanResult.new(issubclass(type_, pd.DataFrame))

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == pd.DataFrame

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        if name := obj.attrs.get("name"):
            return Table(obj, name)
        return super().auto_table(obj)

    @classmethod
    def materialize(
        cls, store: SQLTableStore, table: Table[pd.DataFrame], stage_name: str
    ):
        df = table.obj.copy(deep=False)
        schema = store.get_schema(stage_name)

        if store.print_materialize:
            store.logger.info(
                f"Writing table '{schema.get()}.{table.name}'", table_obj=table.obj
            )

        return cls.materialize_(df, None, store, table, schema)

    @classmethod
    def materialize_(
        cls,
        df: pd.DataFrame,
        dtypes: dict[str:Dtype] | None,
        store: SQLTableStore,
        table: Table[Any],
        schema: Schema,
    ):
        """Helper function that can be invoked by other hooks"""
        if dtypes is None:
            dtypes = {
                name: Dtype.from_pandas(dtype) for name, dtype in df.dtypes.items()
            }

        for col, dtype in dtypes.items():
            # Currently, pandas' .to_sql fails for arrow date columns.
            # -> Temporarily convert all dates to objects
            # See: https://github.com/pandas-dev/pandas/issues/53854
            if pd.__version__ < "2.1.3":
                if dtype == Date():
                    df[col] = df[col].astype(object)

        cls._execute_materialize(
            df,
            store=store,
            table=table,
            schema=schema,
            dtypes=dtypes,
        )

    @classmethod
    def _get_dialect_dtypes(cls, dtypes: dict[str, Dtype], table: Table[pd.DataFrame]):
        _ = table
        return {name: dtype.to_sql() for name, dtype in dtypes.items()}

    @classmethod
    def _dialect_create_empty_table(
        cls,
        store: SQLTableStore,
        df: pd.DataFrame,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, Dtype],
    ):
        df[:0].to_sql(
            table.name,
            store.engine,
            schema=schema.get(),
            index=False,
            dtype=dtypes,
        )

    @classmethod
    def _execute_materialize(
        cls,
        df: pd.DataFrame,
        store: SQLTableStore,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, Dtype],
    ):
        dtypes = cls._get_dialect_dtypes(dtypes, table)
        if table.type_map:
            dtypes.update(table.type_map)

        store.check_materialization_details_supported(
            resolve_materialization_details_label(table)
        )

        if early := store.dialect_requests_empty_creation(table, is_sql=False):
            cls._dialect_create_empty_table(store, df, table, schema, dtypes)
            store.add_indexes_and_set_nullable(
                table, schema, on_empty_table=True, table_cols=df.columns
            )

        with store.engine_connect() as conn:
            with conn.begin():
                if early:
                    store.lock_table(table, schema, conn)
                cls.upload_table(df, table.name, schema.get(), dtypes, conn, early)
        store.add_indexes_and_set_nullable(
            table,
            schema,
            on_empty_table=False if early else None,
            table_cols=df.columns,
        )
        store.optional_pause_for_db_transactionality("table_create")

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[pd.DataFrame] | tuple | dict,
    ) -> pd.DataFrame:
        # Config
        if PandasTableHook.pd_version >= Version("2.0"):
            # Once arrow is mature enough, we might want to switch to
            # arrow backed dataframes by default
            backend_str = "numpy"
        else:
            backend_str = "numpy"

        try:
            if hook_args := ConfigContext.get().table_hook_args.get("pandas", None):
                if dtype_backend := hook_args.get("dtype_backend", None):
                    backend_str = dtype_backend
        except LookupError:
            pass  # in case dematerialization is called without open ConfigContext

        if isinstance(as_type, tuple):
            backend_str = as_type[1]
        elif isinstance(as_type, dict):
            backend_str = as_type["backend"]

        backend = PandasBackend(backend_str)

        # Retrieve
        query, dtypes = cls._build_retrieve_query(store, table, stage_name, backend)
        dataframe = cls._execute_query_retrieve(store, query, dtypes, backend)
        return dataframe

    @classmethod
    def _build_retrieve_query(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        backend: PandasBackend,
    ) -> tuple[Any, dict[str, Dtype]]:
        table_name, schema = store.resolve_alias(table, stage_name)

        sql_table = store.reflect_table(table_name, schema).alias("tbl")

        cols = {col.name: col for col in sql_table.columns}
        dtypes = {name: Dtype.from_sql(col.type) for name, col in cols.items()}

        cols, dtypes = cls._adjust_cols_retrieve(cols, dtypes, backend)

        if cols is None:
            query = sa.select("*").select_from(sql_table)
        else:
            query = sa.select(*cols.values()).select_from(sql_table)
        return query, dtypes

    @classmethod
    def _adjust_cols_retrieve(
        cls, cols: dict, dtypes: dict, backend: PandasBackend
    ) -> tuple[dict | None, dict]:
        # in earlier times pandas only supported datetime64[ns] and thus we implemented
        # clipping in this function to avoid the creation of dtype=object columns
        return None, dtypes
        # return cols, dtypes

    @classmethod
    def _execute_query_retrieve(
        cls,
        store: SQLTableStore,
        query: Any,
        dtypes: dict[str, Dtype],
        backend: PandasBackend,
    ) -> pd.DataFrame:
        dtypes = {name: dtype.to_pandas(backend) for name, dtype in dtypes.items()}

        with store.engine.connect() as conn:
            return cls.download_table(query, conn, dtypes)

    # Auto Version

    class ComputationTracer(ComputationTracer):
        def _monkey_patch(self):
            import numpy
            import pandas

            from pydiverse.common.util.computation_tracing import patch

            for name in sorted(pandas.__all__):
                try:
                    patch(self, pandas, name)
                except TypeError:
                    pass

            for name in sorted(numpy.__all__):
                try:
                    patch(self, numpy, name)
                except TypeError:
                    pass

    @classmethod
    def get_computation_tracer(cls):
        return cls.ComputationTracer()


# endregion

# region POLARS


try:
    import polars
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    polars = None


@SQLTableStore.register_table(polars)
class PolarsTableHook(TableHook[SQLTableStore]):
    @classmethod
    def download_table(
        cls, query: str, connection_uri: str, store: SQLTableStore
    ) -> polars.DataFrame:
        """
        Provide hook that allows to override the default
        download of polars tables from the tablestore.
        """

        # We try to use arrow_odbc (see mssql) or adbc (e.g. adbc-driver-postgresql)
        # if possible. Duckdb also has its own implementation.
        try:
            return polars.read_database_uri(query, connection_uri, engine="adbc")
        except:  # noqa
            # This implementation requires connectorx which does not work for duckdb.
            # Attention: In case this call fails, we simply fall-back to pandas hook.
            return polars.read_database_uri(query, connection_uri)

    @classmethod
    def can_materialize(cls, tbl: Table) -> CanResult:
        # there is a separate hook for LazyFrame
        type_ = type(tbl.obj)
        # attention: tidypolars.Tibble is subclass of polars DataFrame
        return CanResult.new(issubclass(type_, polars.DataFrame))

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == polars.DataFrame

    @classmethod
    def materialize(cls, store, table: Table[polars.DataFrame], stage_name: str):
        # Materialization for polars happens by first converting the dataframe to
        # a pyarrow backed pandas dataframe, and then calling the PandasTableHook
        # for materialization.

        df = table.obj
        schema = store.get_schema(stage_name)

        if store.print_materialize:
            store.logger.info(
                f"Writing table '{schema.get()}.{table.name}'", table_obj=table.obj
            )

        dtypes = dict(zip(df.columns, map(Dtype.from_polars, df.dtypes)))

        try:
            df = _polars_apply_materialize_annotation(df, table)
            e = None
        except Exception as e:
            logger = structlog.get_logger(logger_name=cls.__name__)
            logger.error(
                "Failed to apply materialize annotation for table %s: %s",
                table.name,
                e,
            )
        finally:
            pd_df = df.to_pandas(use_pyarrow_extension_array=True, zero_copy_only=True)
            pandas_hook = store.get_m_table_hook(Table(pd_df))
            ret = pandas_hook.materialize_(
                df=pd_df,
                dtypes=dtypes,
                store=store,
                table=table,
                schema=schema,
            )
            if e:
                raise e
        return ret

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[polars.DataFrame],
    ) -> polars.DataFrame:
        query = cls._read_db_query(store, table, stage_name)
        query = cls._compile_query(store, query)
        connection_uri = store.engine_url.render_as_string(hide_password=False)
        try:
            df = cls._execute_query(query, connection_uri, store)
        except (RuntimeError, ModuleNotFoundError) as e:
            logger = structlog.get_logger(logger_name=cls.__name__)
            logger.error(
                "Fallback via Pandas since Polars failed to execute query on "
                "database %s: %s",
                store.engine_url.render_as_string(hide_password=True),
                e,
            )
            pandas_hook = store.get_r_table_hook(pd.DataFrame)
            with store.engine.connect() as conn:
                pd_df = pandas_hook.download_table(query, conn)
            df = polars.from_pandas(pd_df)
        df = _polars_apply_retrieve_annotation(df, table)
        df = df.with_columns(pl.col(pl.Datetime).dt.replace_time_zone(None))
        return df

    @classmethod
    def auto_table(cls, obj: polars.DataFrame):
        # currently, we don't know how to store a table name inside polars dataframe
        return super().auto_table(obj)

    @classmethod
    def _read_db_query(cls, store: SQLTableStore, table: Table, stage_name: str | None):
        table_name, schema = store.resolve_alias(table, stage_name)

        t = sa.table(table_name, schema=schema)
        q = sa.select("*").select_from(t)

        return q

    @classmethod
    def _compile_query(cls, store: SQLTableStore, query: sa.Select) -> str:
        return str(query.compile(store.engine, compile_kwargs={"literal_binds": True}))

    @classmethod
    def _execute_query(cls, query: str, connection_uri: str, store: SQLTableStore):
        try:
            df = cls.download_table(query, connection_uri, store)
            return df
        except (RuntimeError, ModuleNotFoundError) as e:
            logger = structlog.get_logger(logger_name=cls.__name__)
            engine = sa.create_engine(connection_uri)
            logger.error(
                "Fallback via Pandas since Polars failed to execute query on "
                "database %s: %s",
                engine.url.render_as_string(hide_password=True),
                e,
            )
            pandas_hook = store.get_r_table_hook(pd.DataFrame)
            with engine.connect() as conn:
                pd_df = pandas_hook.download_table(query, conn)
            engine.dispose()
            return polars.from_pandas(pd_df)


@SQLTableStore.register_table(polars)
class LazyPolarsTableHook(TableHook[SQLTableStore]):
    auto_version_support = AutoVersionSupport.LAZY

    @classmethod
    def can_materialize(cls, tbl: Table) -> CanResult:
        type_ = type(tbl.obj)
        return CanResult.new(type_ == polars.LazyFrame)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == polars.LazyFrame

    @classmethod
    def materialize(cls, store, table: Table[polars.LazyFrame], stage_name):
        t = table.obj
        table = table.copy_without_obj()
        table.obj = t.collect()

        polars_hook = store.get_m_table_hook(table)
        return polars_hook.materialize(store, table, stage_name)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[polars.DataFrame],
    ) -> polars.LazyFrame:
        polars_hook = store.get_r_table_hook(pl.DataFrame)
        result = polars_hook.retrieve(
            store=store,
            table=table,
            stage_name=stage_name,
            as_type=as_type,
        )

        return result.lazy()

    @classmethod
    def retrieve_for_auto_versioning_lazy(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type[polars.LazyFrame],
    ) -> polars.LazyFrame:
        polars_hook = store.get_r_table_hook(pl.DataFrame)

        # Retrieve with LIMIT 0 -> only get schema but no data
        query = polars_hook._read_db_query(store, table, stage_name)
        query = query.where(sa.false())  # LIMIT 0
        query = polars_hook._compile_query(store, query)
        connection_uri = store.engine_url.render_as_string(hide_password=False)

        df = polars_hook._execute_query(query, connection_uri, store)
        df = _polars_apply_retrieve_annotation(df, table)

        # Create lazy frame where each column is identified by:
        #     stage name, table name, column name
        # We then rename all columns to match the names of the table.
        #
        # This allows us to properly trace the origin of each column in
        # the output `.serialize` back to the table where it originally came from.

        schema = {}
        rename = {}
        for col in df:
            qualified_name = f"[{table.stage.name}].[{table.name}].[{col.name}]"
            schema[qualified_name] = col.dtype
            rename[qualified_name] = col.name

        lf = polars.LazyFrame(schema=schema).rename(rename)
        return lf

    @classmethod
    def get_auto_version_lazy(cls, obj) -> str:
        """
        :param obj: object returned from task
        :return: string representation of the operations performed on this object.
        :raises TypeError: if the object doesn't support automatic versioning.
        """
        if not isinstance(obj, polars.LazyFrame):
            raise TypeError("Expected LazyFrame")
        return str(obj.serialize())


try:
    import tidypolars
    from tidypolars import Tibble
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    tidypolars = None
    Tibble = None


@SQLTableStore.register_table(tidypolars, polars)
class TidyPolarsTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanResult:
        type_ = type(tbl.obj)
        return CanResult.new(issubclass(type_, Tibble))

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == Tibble

    @classmethod
    def materialize(cls, store, table: Table[Tibble], stage_name):
        t = table.obj
        table = table.copy_without_obj()
        table.obj = t.to_polars()

        polars_hook = store.get_m_table_hook(table)
        return polars_hook.materialize(store, table, stage_name)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[Tibble],
    ) -> Tibble:
        polars_hook = store.get_r_table_hook(pl.DataFrame)
        df = polars_hook.retrieve(store, table, stage_name, as_type)
        return tidypolars.from_polars(df)

    @classmethod
    def auto_table(cls, obj: Tibble):
        # currently, we don't know how to store a table name inside tidypolar   s tibble
        return super().auto_table(obj)


# endregion

# region PYDIVERSE TRANSFORM

try:
    # optional dependency to pydiverse-transform
    import pydiverse.transform as pdt

    try:
        # Detect which version of pdtransform is installed
        # this import only exists in version <0.2
        from pydiverse.transform.eager import PandasTableImpl

        # ensures a "used" state for the import, preventing black from deleting it
        _ = PandasTableImpl

        pdt_old = pdt
        pdt_new = None
    except ImportError:
        try:
            # detect if 0.2 or >0.2 is active
            # this import would only work in <=0.2
            from pydiverse.transform.extended import Polars

            # ensures a "used" state for the import, preventing black from deleting it
            _ = Polars

            pdt_old = None
            pdt_new = pdt
        except ImportError:
            raise NotImplementedError(
                "pydiverse.transform 0.2.0 - 0.2.2 isn't supported"
            ) from None
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    pdt = types.ModuleType("pydiverse.transform")
    pdt.Table = None
    pdt_old = None
    pdt_new = None


@SQLTableStore.register_table(pdt_old)
class PydiverseTransformTableHookOld(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanResult:
        type_ = type(tbl.obj)
        return CanResult.new(issubclass(type_, pdt.Table))

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        return issubclass(type_, (PandasTableImpl, SQLTableImpl))

    @classmethod
    def retrieve_as_reference(cls, type_) -> bool:
        from pydiverse.transform.lazy import SQLTableImpl

        return issubclass(type_, SQLTableImpl)

    @classmethod
    def materialize(cls, store, table: Table[pdt.Table], stage_name):
        from pydiverse.transform.core.verbs import collect
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        t = table.obj
        table = table.copy_without_obj()
        if isinstance(t._impl, PandasTableImpl):
            table.obj = t >> collect()
            hook = store.get_m_table_hook(pd.DataFrame)
            return hook.materialize(store, table, stage_name)
        if isinstance(t._impl, SQLTableImpl):
            table.obj = t._impl.build_select()
            hook = store.get_m_table_hook(table)
            return hook.materialize(store, table, stage_name)
        raise NotImplementedError

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[T],
    ) -> T:
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        if issubclass(as_type, PandasTableImpl):
            hook = store.get_r_table_hook(pd.DataFrame)
            df = hook.retrieve(store, table, stage_name, pd.DataFrame)
            return pdt.Table(PandasTableImpl(table.name, df))
        if issubclass(as_type, SQLTableImpl):
            hook = store.get_r_table_hook(sa.Table)
            sa_tbl = hook.retrieve(store, table, stage_name, sa.Table)
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


@SQLTableStore.register_table(pdt_new)
class PydiverseTransformTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanResult:
        type_ = type(tbl.obj)
        return CanResult.new(issubclass(type_, pdt.Table))

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        from pydiverse.transform.extended import (
            Pandas,
            Polars,
            SqlAlchemy,
        )

        return issubclass(type_, (Polars, SqlAlchemy, Pandas))

    @classmethod
    def retrieve_as_reference(cls, type_) -> bool:
        from pydiverse.transform.extended import SqlAlchemy

        return issubclass(type_, SqlAlchemy)

    @classmethod
    def materialize(cls, store, table: Table[pdt.Table], stage_name):
        from pydiverse.transform.extended import (
            Polars,
            build_query,
            export,
        )

        t = table.obj
        table = table.copy_without_obj()
        query = t >> build_query()
        # detect SQL by checking whether build_query() succeeds
        if query is not None:
            # continue with SQL case handling
            table.obj = sa.text(str(query))
            hook = store.get_m_table_hook(table)
            return hook.materialize(store, table, stage_name)
        else:
            # use Polars for dataframe handling
            table.obj = t >> export(Polars(lazy=True))
            hook = store.get_m_table_hook(table)
            return hook.materialize(store, table, stage_name)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[T],
    ) -> T:
        from pydiverse.transform.extended import Pandas, Polars, SqlAlchemy

        if issubclass(as_type, Polars):
            import polars as pl

            hook = store.get_r_table_hook(pl.LazyFrame)
            lf = hook.retrieve(store, table, stage_name, pl.DataFrame)
            return pdt.Table(lf, name=table.name)
        elif issubclass(as_type, SqlAlchemy):
            hook = store.get_r_table_hook(sa.Table)
            sa_tbl = hook.retrieve(store, table, stage_name, sa.Table)
            return pdt.Table(
                sa_tbl.original.name,
                SqlAlchemy(store.engine, schema=sa_tbl.original.schema),
            )
        elif issubclass(as_type, Pandas):
            import pandas as pd

            hook = store.get_r_table_hook(pd.DataFrame)
            df = hook.retrieve(store, table, stage_name, pd.DataFrame)
            return pdt.Table(df, name=table.name)

        raise NotImplementedError

    @classmethod
    def auto_table(cls, obj: pdt.Table):
        return Table(obj, obj._ast.name)

    @classmethod
    def lazy_query_str(cls, store, obj: pdt.Table) -> str:
        from pydiverse.transform.extended import (
            build_query,
        )

        query = obj >> build_query()

        if query is not None:
            return str(query)
        return super().lazy_query_str(store, obj)


# endregion

# region IBIS


try:
    import ibis
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    ibis = types.ModuleType("ibis")
    ibis.api = types.ModuleType("ibis.api")
    ibis.api.Table = None


@SQLTableStore.register_table(ibis.api.Table)
class IbisTableHook(TableHook[SQLTableStore]):
    @classmethod
    def conn(cls, store: SQLTableStore):
        if conn := store.hook_cache.get((cls, "conn")):
            return conn
        conn = cls._conn(store)
        store.hook_cache[(cls, "conn")] = conn
        return conn

    @classmethod
    def _conn(cls, store: SQLTableStore):
        return ibis.connect(store.engine_url.render_as_string(hide_password=False))

    @classmethod
    def can_materialize(cls, tbl: Table) -> CanResult:
        type_ = type(tbl.obj)
        # Operations on a table like mutate() or join() don't change the type
        return CanResult.new(issubclass(type_, ibis.api.Table))

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return issubclass(type_, ibis.api.Table)

    @classmethod
    def retrieve_as_reference(cls, type_) -> bool:
        return True

    @classmethod
    def materialize(cls, store, table: Table[ibis.api.Table], stage_name):
        t = table.obj
        table = table.copy_without_obj()
        table.obj = sa.text(cls.lazy_query_str(store, t))

        sa_hook = store.get_m_table_hook(table)
        return sa_hook.materialize(store, table, stage_name)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[ibis.api.Table],
    ) -> ibis.api.Table:
        conn = cls.conn(store)
        table_name, schema = store.resolve_alias(table, stage_name)
        for retry_iteration in range(4):
            # retry operation since it might have been terminated as a deadlock victim
            try:
                tbl = conn.table(
                    table_name,
                    database=schema,
                )
                break
            except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError):
                if retry_iteration == 3:
                    raise
                time.sleep(retry_iteration * retry_iteration * 1.2)
        else:
            raise Exception
        return tbl

    @classmethod
    def auto_table(cls, obj: ibis.api.Table):
        if obj.has_name():
            return Table(obj, obj.get_name())
        else:
            return super().auto_table(obj)

    @classmethod
    def lazy_query_str(cls, store, obj: ibis.api.Table) -> str:
        return str(ibis.to_sql(obj, cls.conn(store).name))


# endregion
