# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import inspect
import random
import re
import time
import types
import typing
import warnings
from dataclasses import dataclass
from typing import Any

import numpy as np
import sqlalchemy as sa
import sqlalchemy.exc
from packaging.version import Version
from pandas.core.dtypes.base import ExtensionDtype

from pydiverse.common import Date, Dtype, PandasBackend
from pydiverse.common.util.computation_tracing import ComputationTracer
from pydiverse.common.util.hashing import stable_hash
from pydiverse.pipedag import ConfigContext
from pydiverse.pipedag._typing import T
from pydiverse.pipedag.backend.table.sql.ddl import (
    ChangeTableLogged,
    CreateTableAsSelect,
    DropTable,
    InsertIntoSelect,
)
from pydiverse.pipedag.backend.table.sql.sql import (
    SQLTableStore,
)
from pydiverse.pipedag.container import ExternalTableReference, Schema, Table
from pydiverse.pipedag.context import TaskContext
from pydiverse.pipedag.errors import HookCheckException
from pydiverse.pipedag.materialize.details import resolve_materialization_details_label
from pydiverse.pipedag.materialize.table_hook_base import (
    AutoVersionSupport,
    CanMatResult,
    CanRetResult,
    TableHook,
)
from pydiverse.pipedag.util.sql import compile_sql

# optional imports
try:
    import polars as pl
except ImportError:
    pl = None

try:
    import dataframely as dy
except ImportError:
    dy = None

try:
    import pydiverse.colspec as cs
except ImportError:
    cs = None

# region SQLALCHEMY
try:
    from sqlalchemy import Select, TextClause
    from sqlalchemy import Text as SqlText
except ImportError:
    # For compatibility with sqlalchemy < 2.0
    from sqlalchemy.sql.expression import TextClause
    from sqlalchemy.sql.selectable import Select

    SqlText = TextClause  # this is what sa.text() returns


def _polars_apply_retrieve_annotation(df, table, store, intentionally_empty: bool = False):
    if cs is not None:
        if inspect.isclass(table.annotation) and issubclass(table.annotation, cs.ColSpec):
            column_spec = table.annotation  # type: cs.ColSpec
            if dy is not None:
                # Try colspec polars specific operation which uses dataframely
                # in the back (casting is not done by SQL based colspec)
                try:
                    df = column_spec.cast_polars(df)
                except pl.exceptions.InvalidOperationError as e:
                    df, failures = column_spec.filter_polars(df, cast=True)
                    with pl.Config() as cfg:
                        cfg.set_tbl_cols(15)
                        cfg.set_tbl_width_chars(120)
                        try:
                            fail_df = str(failures._lf.head(5).collect())
                        except:  # noqa
                            fail_df = str(failures.invalid().head(5))
                    raise HookCheckException(
                        f"Failed casting polars input '{table.name}' to "
                        f"{column_spec.__name__}; "
                        f"Failure counts: {failures.counts()}; "
                        f"\nInvalid:\n{fail_df}"
                    ) from e
    if dy is not None:
        if typing.get_origin(table.annotation) is not None and issubclass(
            typing.get_origin(table.annotation), dy.LazyFrame | dy.DataFrame
        ):
            anno_args = typing.get_args(table.annotation)
            if len(anno_args) == 1:
                column_spec = anno_args[0]
                if issubclass(column_spec, dy.Schema):
                    try:
                        df = column_spec.cast(df)
                    except pl.exceptions.InvalidOperationError as e:
                        df, failures = column_spec.filter(df, cast=True)
                        with pl.Config() as cfg:
                            cfg.set_tbl_cols(15)
                            cfg.set_tbl_width_chars(120)
                            try:
                                fail_df = str(failures._lf.head(5).collect())
                            except:  # noqa
                                fail_df = str(failures.invalid().head(5))
                        raise HookCheckException(
                            f"Failed casting polars input '{table.name}' to "
                            f"{column_spec.__name__}; "
                            f"Failure counts: {failures.counts()}; "
                            f"\nInvalid:\n{fail_df}"
                        ) from e
    return df


def _polars_apply_materialize_annotation(df, table, store):
    if dy is not None:
        if typing.get_origin(table.annotation) is not None and issubclass(
            typing.get_origin(table.annotation), dy.LazyFrame | dy.DataFrame
        ):
            anno_args = typing.get_args(table.annotation)
            if len(anno_args) == 1:
                column_spec = anno_args[0]
                if issubclass(column_spec, dy.Schema):
                    df, failures = column_spec.filter(df, cast=True)
                    if len(failures) > 0:
                        with pl.Config() as cfg:
                            cfg.set_tbl_cols(15)
                            cfg.set_tbl_width_chars(120)
                            try:
                                fail_df = str(failures._lf.head(5).collect())
                            except:  # noqa
                                fail_df = str(failures.invalid().head(5))
                        raise HookCheckException(
                            f"Polars task output {table.name} failed "
                            f"validation with {column_spec.__name__}; "
                            f"Failure counts: {failures.counts()}; "
                            f"\nInvalid:\n{fail_df}"
                        )
    if cs is not None:
        if inspect.isclass(table.annotation) and issubclass(table.annotation, cs.ColSpec):
            column_spec = table.annotation
            if dy is not None:
                # Try colspec polars specific operation which uses dataframely
                # in the back
                df, failures = column_spec.filter_polars(df, cast=True)
                if len(failures) > 0:
                    with pl.Config() as cfg:
                        cfg.set_tbl_cols(15)
                        cfg.set_tbl_width_chars(120)
                        try:
                            fail_df = str(failures._lf.head(5).collect())
                        except:  # noqa
                            fail_df = str(failures.invalid().head(5))
                    raise HookCheckException(
                        f"Polars task output {table.name} failed "
                        f"validation with {column_spec.__name__}; "
                        f"Failure counts: {failures.counts()}; "
                        f"\nInvalid:\n{fail_df}"
                    )
            elif pdt_new is not None:
                tbl, failures = column_spec.filter(pdt.Table(df), cast=True)
                if len(failures) > 0:
                    with pl.Config() as cfg:
                        cfg.set_tbl_cols(15)
                        cfg.set_tbl_width_chars(120)
                        debug_invalid_rows = failures.debug_invalid_rows
                        if debug_invalid_rows >> pdt.build_query() and store.engine.dialect.name == "mssql":
                            # this is just a workaround as long as pydiverse.transform
                            # puts an OFFSET in slice_head
                            debug_invalid_rows = failures.debug_invalid_rows >> pdt.arrange(pdt.lit(True))

                        fail_df = str(debug_invalid_rows >> pdt.slice_head(5) >> pdt.export(Polars(lazy=False)))
                    raise HookCheckException(
                        f"Polars task output {table.name} failed "
                        f"validation with {column_spec.__name__}; "
                        f"Failure counts: {failures.counts()}; "
                        f"\nInvalid:\n{fail_df}"
                    )
                df = tbl >> pdt.export(Polars(lazy=False))

    return df


def _sql_apply_materialize_annotation_pdt_early(table: Table, schema: Schema, store: SQLTableStore):
    tbl = table.obj  # type: pdt.Table
    if cs is not None:
        if inspect.isclass(table.annotation) and issubclass(table.annotation, cs.ColSpec):
            column_spec = table.annotation
            if pdt_new is not None:
                # cast columns according to column specification
                # Enum and Struct are currently not supported by pydiverse libraries
                cols = set(c.name for c in tbl)
                tbl = tbl >> pdt.mutate(
                    **{
                        name: tbl[name].cast(col.dtype())
                        for name, col in column_spec.columns().items()
                        if name in cols and not isinstance(col, cs.Enum) and not isinstance(col, cs.Struct)
                    }
                )
    return tbl


def _sql_apply_materialize_annotation(
    table: Table,
    schema: Schema,
    query,
    store: SQLTableStore,
    suffix: str | None,
    unlogged: bool,
) -> tuple[str | None, list[str] | None]:
    invalid_rows = None  # type: str | None
    intermediate_tbls = None  # type: list[str] | None

    def write_pdt_table(tbl: pdt.Table, table_name: str, suffix: str | None = None):
        query = sa.text(str(tbl >> pdt.build_query()))
        schema = store.write_subquery(query, table_name, neighbor_table=table, unlogged=unlogged, suffix=suffix)
        return pdt.Table(table_name, pdt.SqlAlchemy(store.engine, schema=schema.get()))

    def materialize_hook(tbl: pdt.Table, table_prefix):
        name = table_prefix or ""
        name += tbl._ast.name or ""
        name += stable_hash(str(random.randbytes(8)))
        intermediate_tbls.append(name)
        return write_pdt_table(tbl, name)

    if cs is not None:
        if inspect.isclass(table.annotation) and issubclass(table.annotation, cs.ColSpec):
            column_spec = table.annotation
            if pdt_new is not None:
                supported_dialects = ["duckdb", "sqlite", "postgresql", "mssql"]
                if store.engine.dialect.name in supported_dialects:
                    intermediate_tbls = []
                    cfg = cs.config.Config.default
                    cfg.dialect_name = store.engine.dialect.name
                    cfg.materialize_hook = materialize_hook
                    tmp_name = "_raw_" + table.name
                    store.rename_table(table, tmp_name, schema)
                    raw = pdt.Table(tmp_name, pdt.SqlAlchemy(store.engine, schema=schema.get()))
                    tbl, failures = column_spec.filter(raw, cast=True, cfg=cfg)
                    write_pdt_table(tbl, table.name, suffix)
                    failure_counts = failures.counts()
                    if len(failure_counts) > 0:
                        with pl.Config() as cfg:
                            cfg.set_tbl_cols(15)
                            cfg.set_tbl_width_chars(120)
                            if store.engine.dialect.name == "mssql":
                                # this is just a workaround as long as pydiverse.transform
                                # puts an OFFSET in slice_head
                                debug_invalid_rows = failures.debug_invalid_rows >> pdt.arrange(pdt.lit(True))
                            else:
                                debug_invalid_rows = failures.debug_invalid_rows
                            fail_df = str(debug_invalid_rows >> pdt.slice_head(5) >> pdt.export(Polars(lazy=False)))
                        raise HookCheckException(
                            f"Sql task output {table.name} failed "
                            f"validation with {column_spec.__name__}; "
                            f"Failure counts: {failure_counts}; "
                            f"\nInvalid:\n{fail_df}\nQuery:\n{compile_sql(query)}"
                        )
                    invalid_rows = tbl >> pdt.alias(table.name)
                else:
                    store.logger.info(
                        "Colspec annotation ignored because pydiverse.transform currently does not support SQL dialect",
                        dialect=store.engine.dialect.name,
                        supported=supported_dialects,
                    )

    return invalid_rows, intermediate_tbls


@SQLTableStore.register_table()
class SQLAlchemyTableHook(TableHook[SQLTableStore]):
    @dataclass  # consider using pydantic instead
    class Config:
        disable_materialize_annotation_action: bool = False
        disable_retrieve_annotation_action: bool = False
        cleanup_annotation_action_on_success: bool = False
        cleanup_annotation_action_intermediate_state: bool = True
        fault_tolerant_annotation_action: bool = False

    @classmethod
    def cfg(cls) -> Config:
        ret = SQLAlchemyTableHook.Config()
        if hook_args := ConfigContext.get().table_hook_args.get("sql", None):
            for key, value in hook_args.items():
                if hasattr(ret, key):
                    if type(getattr(ret, key)) is not type(value):
                        raise TypeError(
                            f"Invalid type for polars hook argument '{key}': "
                            f"expected {type(getattr(ret, key))}, got {type(value)}"
                        )
                    setattr(ret, key, value)
                else:
                    raise ValueError(f"Unknown polars hook argument '{key}' in table_hook_args.")
        return ret

    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        return CanMatResult.new(issubclass(type_, (sa.sql.expression.TextClause, sa.sql.expression.Selectable)))

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        return CanRetResult.new(type_ == sa.Table)

    @classmethod
    def retrieve_as_reference(cls, type_):
        return True

    @classmethod
    def materialize(
        cls,
        store: SQLTableStore,
        table: Table[sa.sql.expression.TextClause | sa.sql.expression.Selectable],
        stage_name,
        without_config_context: bool = False,
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

        store.check_materialization_details_supported(resolve_materialization_details_label(table))

        suffix = store.get_create_table_suffix(resolve_materialization_details_label(table))
        unlogged = store.get_unlogged(resolve_materialization_details_label(table))
        if store.dialect_requests_empty_creation(table, is_sql=True):
            cls._create_table_as_select_empty_insert(table, schema, query, source_tables, store, suffix, unlogged)
        else:
            cls._create_table_as_select(table, schema, query, source_tables, store, suffix, unlogged)
        if not without_config_context:
            cfg = cls.cfg()
            invalid_rows, intermediate_tbls = None, None
            if not cfg.disable_materialize_annotation_action:
                try:
                    invalid_rows, intermediate_tbls = _sql_apply_materialize_annotation(
                        table, schema, query, store, suffix, unlogged
                    )
                except Exception as e:  # noqa
                    store.logger.error(
                        "Failed to apply materialize annotation for table",
                        table=table.name,
                        exception=str(e),
                    )
                    if not cfg.fault_tolerant_annotation_action:
                        raise e
            if cfg.cleanup_annotation_action_on_success and invalid_rows is not None:
                store.logger.debug(
                    "Cleaning up intermediate state after successful materialization",
                    table=table.name,
                )
                store.execute(
                    store.execute(DropTable(invalid_rows, schema, if_exists=True)),
                    truncate_printed_select=True,
                )
            if cfg.cleanup_annotation_action_intermediate_state and intermediate_tbls is not None:
                store.logger.debug(
                    "Cleaning up intermediate state after materialization",
                    table=table.name,
                )
                for tbl in intermediate_tbls:
                    store.drop_subquery_table(tbl, schema, neighbor_table=table, if_exists=True)

        store.optional_pause_for_db_transactionality("table_create")

    @classmethod
    def _create_table_as_select(
        cls,
        table: Table,
        schema: Schema,
        query: Select | TextClause | SqlText,
        source_tables: list[dict[str, str]],
        store: SQLTableStore,
        suffix: str,
        unlogged: bool,
    ):
        statements = store.lock_source_tables(source_tables)
        statements += cls._create_as_select_statements(table.name, schema, query, store, suffix, unlogged)
        store.execute(statements)
        store.add_indexes_and_set_nullable(table, schema)

    @classmethod
    def _create_table_as_select_empty_insert(
        cls,
        table: Table,
        schema: Schema,
        query: Select | TextClause | SqlText,
        source_tables: list[dict[str, str]],
        store: SQLTableStore,
        suffix: str,
        unlogged: bool,
    ):
        limit_query = store.get_limit_query(query, rows=0)
        store.execute(cls._create_as_select_statements(table.name, schema, limit_query, store, suffix, unlogged))
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
        query: Select | TextClause | SqlText,
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
        query: Select | TextClause | SqlText,
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
        limit: int | None = None,
    ) -> sa.sql.expression.Selectable:
        assert limit is None, "SQLAlchemyTableHook does not support limit in retrieve."
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
        query_str = str(query.compile(store.engine, compile_kwargs={"literal_binds": True}))
        # hacky way to canonicalize query (despite __tmp/__even/__odd suffixes
        # and alias resolution)
        query_str = re.sub(r'["\[\]]', "", query_str)
        query_str = re.sub(r'(__tmp|__even|__odd)(?=[ \t\n.;"]|$)', "", query_str.lower())
        return query_str


@SQLTableStore.register_table()
class ExternalTableReferenceHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        return CanMatResult.new(issubclass(type_, ExternalTableReference))

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        return CanRetResult.NO

    @classmethod
    def materialize(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        without_config_context: bool = False,
    ):
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


class DataframeSqlTableHook:
    """
    Base class for hooks that handle pandas or polars DataFrames.
    Provides common functionality for uploading and downloading tables.
    """

    @classmethod
    def dialect_has_adbc_driver(cls):
        # by default, we assume an ADBC driver exists
        return True

    @classmethod
    def download_table(
        cls,
        query: Any,
        store: SQLTableStore,
        dtypes: dict[str, ExtensionDtype | np.dtype] | None = None,
    ) -> Any:
        raise NotImplementedError("This method must be implemented by subclasses.")

    @classmethod
    def upload_table(
        cls,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, sa.types.TypeEngine],
        store: SQLTableStore,
        early: bool,
    ):
        """
        Provide hook that allows to override the default
        upload of pandas/polars tables to the tablestore.
        """
        raise NotImplementedError("This method must be implemented by subclasses.")

    @classmethod
    def _get_dialect_dtypes(
        cls, dtypes: dict[str, Dtype], table: Table[pd.DataFrame]
    ) -> dict[str, sa.types.TypeEngine]:
        """
        Convert dtypes to SQLAlchemy types.
        """
        sql_dtypes = {name: dtype.to_sql() for name, dtype in dtypes.items()}
        if table.type_map:
            sql_dtypes.update(table.type_map)
        return sql_dtypes

    @classmethod
    def _dialect_create_empty_table(
        cls,
        store: SQLTableStore,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, sa.types.TypeEngine],
    ):
        """
        Create an empty table in the database.
        """
        raise NotImplementedError("This method must be implemented by subclasses.")

    @classmethod
    def get_columns(cls, df):
        return df.columns

    @classmethod
    def _execute_materialize(
        cls,
        table: Table,
        store: SQLTableStore,
        schema: Schema,
        dtypes: dict[str, Dtype],
    ):
        df = table.obj
        dtypes = cls._get_dialect_dtypes(dtypes, table)

        store.check_materialization_details_supported(resolve_materialization_details_label(table))

        if early := store.dialect_requests_empty_creation(table, is_sql=False):
            cls._dialect_create_empty_table(store, table, schema, dtypes)
            store.add_indexes_and_set_nullable(table, schema, on_empty_table=True, table_cols=cls.get_columns(df))
            if store.get_unlogged(resolve_materialization_details_label(table)):
                store.execute(ChangeTableLogged(table.name, schema, False))

        cls.upload_table(table, schema, dtypes, store, early)
        store.add_indexes_and_set_nullable(
            table,
            schema,
            on_empty_table=False if early else None,
            table_cols=cls.get_columns(df),
        )
        store.optional_pause_for_db_transactionality("table_create")

    @classmethod
    def _build_retrieve_query(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        limit: int | None = None,
    ) -> tuple[Any, dict[str, Dtype]]:
        table_name, schema = store.resolve_alias(table, stage_name)

        sql_table = store.reflect_table(table_name, schema).alias("tbl")

        cols = {col.name: col for col in sql_table.columns}
        dtypes = {name: Dtype.from_sql(col.type) for name, col in cols.items()}

        cols, dtypes = cls._adjust_cols_retrieve(store, cols, dtypes)

        if cols is None:
            query = sa.select("*").select_from(sql_table)
        else:
            query = sa.select(*cols.values()).select_from(sql_table)
        if limit is not None:
            query = store.get_limit_query(query, rows=limit)
        return query, dtypes

    @classmethod
    def _adjust_cols_retrieve(cls, store: SQLTableStore, cols: dict, dtypes: dict) -> tuple[dict | None, dict]:
        # in earlier times pandas only supported datetime64[ns] and thus we implemented
        # clipping in this function to avoid the creation of dtype=object columns
        return None, dtypes
        # return cols, dtypes


@SQLTableStore.register_table(pd)
class PandasTableHook(TableHook[SQLTableStore], DataframeSqlTableHook):
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
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, sa.types.TypeEngine],
        store: SQLTableStore,
        early: bool,
    ):
        """
        Provide hook that allows to override the default
        upload of pandas/polars tables to the tablestore.
        """
        df = table.obj
        schema_name = schema.get()
        with store.engine_connect() as conn:
            with conn.begin():
                if early:
                    store.lock_table(table, schema_name, conn)
                df.to_sql(
                    table.name,
                    conn,
                    schema=schema_name,
                    index=False,
                    dtype=dtypes,
                    chunksize=100_000,
                    if_exists="append" if early else "fail",
                )

    @classmethod
    def download_table(
        cls,
        query: Any,
        store: SQLTableStore,
        dtypes: dict[str, ExtensionDtype | np.dtype] | None = None,
    ) -> pd.DataFrame:
        """
        Provide hook that allows to override the default
        download of pandas tables from the tablestore.
        Also serves as fallback for polars download.
        """
        with store.engine.connect() as conn:
            if PandasTableHook.pd_version >= Version("2.0"):
                df = pd.read_sql(query, con=conn, dtype=dtypes)
            else:
                df = pd.read_sql(query, con=conn)
                df = cls._fix_dtypes(df, dtypes)
            return df

    @classmethod
    def _fix_dtypes(
        cls,
        df: pd.DataFrame,
        dtypes: dict[str, ExtensionDtype | np.dtype] | None = None,
    ) -> pd.DataFrame:
        # df = df.copy(deep=False)  would be costly and is typically not needed
        if dtypes is not None:
            for col, dtype in dtypes.items():
                df[col] = df[col].astype(dtype)
        return df

    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        return CanMatResult.new(issubclass(type_, pd.DataFrame))

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        return CanRetResult.new(type_ == pd.DataFrame)

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        if name := obj.attrs.get("name"):
            return Table(obj, name)
        return super().auto_table(obj)

    @classmethod
    def materialize(
        cls,
        store: SQLTableStore,
        table: Table[pd.DataFrame],
        stage_name: str,
        without_config_context: bool = False,
    ):
        df = table.obj.copy(deep=False)
        schema = store.get_schema(stage_name)

        if store.print_materialize:
            store.logger.info(f"Writing table '{schema.get()}.{table.name}'", table_obj=table.obj)

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
            dtypes = {name: Dtype.from_pandas(dtype) for name, dtype in df.dtypes.items()}

        for col, dtype in dtypes.items():
            # Currently, pandas' .to_sql fails for arrow date columns.
            # -> Temporarily convert all dates to objects
            # See: https://github.com/pandas-dev/pandas/issues/53854
            if pd.__version__ < "2.1.3":
                if dtype == Date():
                    df[col] = df[col].astype(object)

        table = table.copy_without_obj()
        table.obj = df

        cls._execute_materialize(
            table=table,
            store=store,
            schema=schema,
            dtypes=dtypes,
        )

    @classmethod
    def _dialect_create_empty_table(
        cls,
        store: SQLTableStore,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, sa.types.TypeEngine],
    ):
        df = table.obj  # type: pd.DataFrame
        df[:0].to_sql(
            table.name,
            store.engine,
            schema=schema.get(),
            index=False,
            dtype=dtypes,
        )

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[pd.DataFrame] | tuple | dict,
        limit: int | None = None,
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
        query, dtypes = cls._build_retrieve_query(store, table, stage_name, limit)
        dataframe = cls._execute_query_retrieve(store, query, dtypes, backend)
        return dataframe

    @classmethod
    def _execute_query_retrieve(
        cls,
        store: SQLTableStore,
        query: Any,
        dtypes: dict[str, Dtype],
        backend: PandasBackend,
    ) -> pd.DataFrame:
        dtypes = {name: dtype.to_pandas(backend) for name, dtype in dtypes.items()}

        return cls.download_table(query, store, dtypes)

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


@SQLTableStore.register_table(pl)
class PolarsTableHook(TableHook[SQLTableStore], DataframeSqlTableHook):
    @dataclass  # consider using pydantic instead
    class Config:
        disable_materialize_annotation_action: bool = False
        disable_retrieve_annotation_action: bool = False
        fault_tolerant_annotation_action: bool = False

    @classmethod
    def cfg(cls):
        ret = PolarsTableHook.Config()
        if hook_args := ConfigContext.get().table_hook_args.get("polars", None):
            for key, value in hook_args.items():
                if hasattr(ret, key):
                    if type(getattr(ret, key)) is not type(value):
                        raise TypeError(
                            f"Invalid type for polars hook argument '{key}': "
                            f"expected {type(getattr(ret, key))}, got {type(value)}"
                        )
                    setattr(ret, key, value)
                else:
                    raise ValueError(f"Unknown polars hook argument '{key}' in table_hook_args.")
        return ret

    @classmethod
    def _dialect_create_empty_table(
        cls,
        store: SQLTableStore,
        table: Table[pl.DataFrame],
        schema: Schema,
        dtypes: dict[str, sa.types.TypeEngine],
    ):
        df = table.obj  # type: pl.DataFrame
        pd_df = df.to_pandas(use_pyarrow_extension_array=True, zero_copy_only=True)
        pd_df[:0].to_sql(
            table.name,
            store.engine,
            schema=schema.get(),
            index=False,
            dtype=dtypes,
        )
        # _ = dtypes  # unfortunately, we don't know how to set dtypes with polars, yet
        # engine = store.engine
        # table_name = engine.dialect.identifier_preparer.quote(table.name)
        # schema_name = engine.dialect.identifier_preparer.format_schema(schema.get())
        # connection_uri = store.engine_url.render_as_string(hide_password=False)
        # df.slice(0, 0).write_database(
        #     f"{schema_name}.{table_name}",
        #     connection_uri,
        #     if_table_exists="replace",
        #     engine="adbc",
        # )

    @classmethod
    def upload_table(
        cls,
        table: Table[pl.DataFrame],
        schema: Schema,
        dtypes: dict[str, sa.types.TypeEngine],
        store: SQLTableStore,
        early: bool,
    ):
        # use ADBC for writing by default
        df = table.obj  # type: pl.DataFrame
        engine = store.engine
        table_name = engine.dialect.identifier_preparer.quote(table.name)
        schema_name = engine.dialect.identifier_preparer.format_schema(schema.get())
        if not early:
            # as long as we don't know how to provide dtypes to write_database, we need
            # to create the table first
            cls._dialect_create_empty_table(store, table, schema, dtypes)
        if cls.dialect_has_adbc_driver():
            try:
                # try using ADBC, first
                return df.write_database(
                    f"{schema_name}.{table_name}",
                    engine.url.render_as_string(hide_password=False),
                    if_table_exists="append",
                    engine="adbc",
                )
            except Exception as e:  # noqa
                store.logger.warning(
                    "Failed writing table using ADBC, falling back to sqlalchemy: %s",
                    table.name,
                )
        df.write_database(
            f"{schema_name}.{table_name}",
            engine,
            if_table_exists="append",
        )

    @classmethod
    def download_table(
        cls,
        query: Any,
        store: SQLTableStore,
        dtypes: dict[str, pl.DataType] | None = None,
    ) -> pl.DataFrame:
        """
        Provide hook that allows to override the default
        download of polars tables from the tablestore.
        """
        assert dtypes is None, (
            "Polars reads SQL schema and loads the data in reasonable types."
            "Thus, manual dtype manipulation can only done via query or afterwards."
        )
        # We try to use arrow_odbc (see mssql) or adbc (e.g. adbc-driver-postgresql)
        # if possible. Duckdb also has its own implementation.
        connection_uri = store.engine_url.render_as_string(hide_password=False)
        if cls.dialect_has_adbc_driver():
            try:
                df = pl.read_database_uri(query, connection_uri, engine="adbc")
                return cls._fix_dtypes(df, dtypes)
            except:  # noqa
                store.logger.warning(
                    "Failed retrieving query using ADBC, falling back to Pandas: %s",
                    query,
                )
        # This implementation requires connectorx which does not work for duckdb.
        # Attention: In case this call fails, we simply fall-back to pandas hook.
        df = pl.read_database_uri(query, connection_uri)
        return cls._fix_dtypes(df, dtypes)

    @classmethod
    def _execute_materialize_polars(
        cls,
        table: Table,
        store: SQLTableStore,
        stage_name: str,
    ):
        df = table.obj
        schema = store.get_schema(stage_name)
        dtypes = {name: Dtype.from_polars(dtype) for name, dtype in df.collect_schema().items()}

        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        table = table.copy_without_obj()
        table.obj = df
        cls._execute_materialize(table, store, schema, dtypes)

    @classmethod
    def _fix_dtypes(cls, df: pl.DataFrame, dtypes: dict[str, pl.DataType] | None = None) -> pl.DataFrame:
        if dtypes is not None:
            return df.with_columns(**{col: df[col].cast(dtype) for col, dtype in dtypes.items()})
        return df

    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        # there is a separate hook for LazyFrame
        type_ = type(tbl.obj)
        # attention: tidypolars.Tibble is subclass of polars DataFrame
        return CanMatResult.new(issubclass(type_, pl.DataFrame))

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        return CanRetResult.new(type_ == pl.DataFrame)

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[pl.DataFrame],
        stage_name: str,
        without_config_context: bool = False,
    ):
        # Materialization for polars happens by first converting the dataframe to
        # a pyarrow backed pandas dataframe, and then calling the PandasTableHook
        # for materialization.

        df = table.obj

        if store.print_materialize:
            schema = store.get_schema(stage_name)
            store.logger.info(f"Writing table '{schema.get()}.{table.name}'", table_obj=table.obj)

        cfg = cls.cfg()
        try:
            if not cfg.disable_materialize_annotation_action:
                df = _polars_apply_materialize_annotation(df, table, store)
            ex = None
        except Exception as e:
            store.logger.error(
                "Failed to apply materialize annotation for table",
                table=table.name,
                exception=str(e),
            )
            ex = e
        finally:
            table = table.copy_without_obj()
            table.obj = df
            cls._execute_materialize_polars(table, store, stage_name)
            if ex and not cfg.fault_tolerant_annotation_action:
                raise ex

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[pl.DataFrame],
        limit: int | None = None,
    ) -> pl.DataFrame:
        cfg = cls.cfg()
        df = cls._execute_query(store, table, stage_name, as_type, limit=limit)
        if not cfg.disable_retrieve_annotation_action:
            try:
                df = _polars_apply_retrieve_annotation(df, table, store)
            except Exception as e:
                store.logger.error(
                    "Failed to apply retrieve annotation for table",
                    table=table.name,
                    exception=e,
                )
                if not cfg.fault_tolerant_annotation_action:
                    raise e

        df = df.with_columns(pl.col(pl.Datetime).dt.replace_time_zone(None))
        return df

    @classmethod
    def auto_table(cls, obj: pl.DataFrame):
        # currently, we don't know how to store a table name inside polars dataframe
        return super().auto_table(obj)

    @classmethod
    def _compile_query(cls, store: SQLTableStore, query: Select) -> str:
        return str(query.compile(store.engine, compile_kwargs={"literal_binds": True}))

    @classmethod
    def _execute_query(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type,
        limit: int | None = None,
    ) -> pl.DataFrame:
        _ = as_type
        query, dtypes = cls._build_retrieve_query(store, table, stage_name, limit)
        # Polars database read methods tend to do a good job in automatic type mapping.
        # Thus dtypes must be corrected after loading if needed.
        dtypes = None
        query = cls._compile_query(store, query)
        try:
            df = cls.download_table(query, store, dtypes)
        except (RuntimeError, ModuleNotFoundError) as e:
            store.logger.error(
                "Fallback via Pandas since Polars failed to execute query on database %s: %s",
                store.engine_url.render_as_string(hide_password=True),
                e,
            )
            pandas_hook = store.get_r_table_hook(pd.DataFrame)  # type: PandasTableHook
            backend = PandasBackend.ARROW
            query, dtypes = pandas_hook._build_retrieve_query(store, table, stage_name)
            dtypes = {name: dtype.to_pandas(backend) for name, dtype in dtypes.items()}
            pd_df = pandas_hook.download_table(query, store, dtypes)
            df = pl.from_pandas(pd_df)
        return df


@SQLTableStore.register_table(pl)
class LazyPolarsTableHook(TableHook[SQLTableStore]):
    auto_version_support = AutoVersionSupport.LAZY

    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        return CanMatResult.new(type_ == pl.LazyFrame)

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        if dy is not None and issubclass(type_, dy.LazyFrame):
            # optionally support input_type=dy.LazyFrame
            return CanRetResult.YES
        return CanRetResult.new(type_ == pl.LazyFrame)

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[pl.LazyFrame],
        stage_name,
        without_config_context: bool = False,
    ):
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
        as_type: type[pl.DataFrame],
        limit: int | None = None,
    ) -> pl.LazyFrame:
        polars_hook = store.get_r_table_hook(pl.DataFrame)
        result = polars_hook.retrieve(store, table, stage_name, as_type, limit)

        return result.lazy()

    @classmethod
    def retrieve_for_auto_versioning_lazy(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type[pl.LazyFrame],
    ) -> pl.LazyFrame:
        polars_hook = store.get_r_table_hook(pl.DataFrame)  # type: PolarsTableHook
        df = polars_hook.retrieve(store, table, stage_name, as_type, limit=0)

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

        lf = pl.LazyFrame(schema=schema).rename(rename)
        return lf

    @classmethod
    def get_auto_version_lazy(cls, obj) -> str:
        """
        :param obj: object returned from task
        :return: string representation of the operations performed on this object.
        :raises TypeError: if the object doesn't support automatic versioning.
        """
        if not isinstance(obj, pl.LazyFrame):
            raise TypeError("Expected LazyFrame")
        return str(obj.serialize())


try:
    import tidypolars
    from tidypolars import Tibble
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    tidypolars = None
    Tibble = None


@SQLTableStore.register_table(tidypolars, pl)
class TidyPolarsTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        return CanMatResult.new(issubclass(type_, Tibble))

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        return CanRetResult.new(type_ == Tibble)

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[Tibble],
        stage_name,
        without_config_context: bool = False,
    ):
        warnings.warn(
            "tidypolars support is deprecated since tidypolars does not work with current version of polars",
            DeprecationWarning,
            stacklevel=2,
        )
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
        limit: int | None = None,
    ) -> Tibble:
        warnings.warn(
            "tidypolars support is deprecated since tidypolars does not work with current version of polars",
            DeprecationWarning,
            stacklevel=2,
        )
        polars_hook = store.get_r_table_hook(pl.DataFrame)
        df = polars_hook.retrieve(store, table, stage_name, as_type, limit)
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
            raise NotImplementedError("pydiverse.transform 0.2.0 - 0.2.2 isn't supported") from None
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    pdt = types.ModuleType("pydiverse.transform")
    pdt.Table = None
    pdt_old = None
    pdt_new = None


@SQLTableStore.register_table(pdt_old)
class PydiverseTransformTableHookOld(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        return CanMatResult.new(issubclass(type_, pdt.Table))

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        return CanRetResult.new(issubclass(type_, (PandasTableImpl, SQLTableImpl)))

    @classmethod
    def retrieve_as_reference(cls, type_) -> bool:
        from pydiverse.transform.lazy import SQLTableImpl

        return issubclass(type_, SQLTableImpl)

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[pdt.Table],
        stage_name,
        without_config_context: bool = False,
    ):
        from pydiverse.transform.core.verbs import collect
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        t = table.obj
        table = table.copy_without_obj()
        if isinstance(t._impl, PandasTableImpl):
            table.obj = t >> collect()
            hook = store.get_m_table_hook(table)
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
        limit: int | None = None,
    ) -> T:
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        if issubclass(as_type, PandasTableImpl):
            hook = store.get_r_table_hook(pd.DataFrame)
            df = hook.retrieve(store, table, stage_name, pd.DataFrame, limit)
            return pdt.Table(PandasTableImpl(table.name, df))
        if issubclass(as_type, SQLTableImpl):
            hook = store.get_r_table_hook(sa.Table)
            sa_tbl = hook.retrieve(store, table, stage_name, sa.Table, limit)
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
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        return CanMatResult.new(issubclass(type_, pdt.Table))

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        from pydiverse.transform.extended import (
            Pandas,
            Polars,
            SqlAlchemy,
        )

        return CanRetResult.new(issubclass(type_, (Polars, SqlAlchemy, Pandas)))

    @classmethod
    def retrieve_as_reference(cls, type_) -> bool:
        from pydiverse.transform.extended import SqlAlchemy

        return issubclass(type_, SqlAlchemy)

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[pdt.Table],
        stage_name,
        without_config_context: bool = False,
    ):
        from pydiverse.transform.extended import (
            Polars,
            build_query,
            export,
        )

        t = table.obj
        table_cpy = table.copy_without_obj()
        query = t >> build_query()
        # detect SQL by checking whether build_query() succeeds
        if query is not None:
            # continue with SQL case handling
            table_cpy.obj = sa.text(str(query))
            hook = store.get_m_table_hook(table_cpy)
            assert hook, "fatal error: no hook for materialization of SqlAlchemy query found"
            cfg = hook.cfg()
            if not cfg.disable_materialize_annotation_action:
                schema = store.get_schema(stage_name)
                t = _sql_apply_materialize_annotation_pdt_early(table, schema, store)
                table_cpy.obj = sa.text(str(t >> build_query()))
            return hook.materialize(store, table_cpy, stage_name)
        else:
            # use Polars for dataframe handling
            table_cpy.obj = t >> export(Polars(lazy=True))
            hook = store.get_m_table_hook(table_cpy)
            return hook.materialize(store, table_cpy, stage_name)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[T],
        limit: int | None = None,
    ) -> T:
        from pydiverse.transform.extended import Pandas, Polars, SqlAlchemy

        if issubclass(as_type, Polars):
            import polars as pl

            hook = store.get_r_table_hook(pl.LazyFrame)
            lf = hook.retrieve(store, table, stage_name, pl.DataFrame, limit)
            return pdt.Table(lf, name=table.name)
        elif issubclass(as_type, SqlAlchemy):
            hook = store.get_r_table_hook(sa.Table)
            sa_tbl = hook.retrieve(store, table, stage_name, sa.Table, limit)
            return pdt.Table(
                sa_tbl.original.name,
                SqlAlchemy(store.engine, schema=sa_tbl.original.schema),
            )
        elif issubclass(as_type, Pandas):
            import pandas as pd

            hook = store.get_r_table_hook(pd.DataFrame)
            df = hook.retrieve(store, table, stage_name, pd.DataFrame, limit)
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
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        # Operations on a table like mutate() or join() don't change the type
        return CanMatResult.new(issubclass(type_, ibis.api.Table))

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        return CanRetResult.new(issubclass(type_, ibis.api.Table))

    @classmethod
    def retrieve_as_reference(cls, type_) -> bool:
        return True

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[ibis.api.Table],
        stage_name,
        without_config_context: bool = False,
    ):
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
        limit: int | None = None,
    ) -> ibis.api.Table:
        assert limit is None, "IbisTableHook does not support limit in retrieve."
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
