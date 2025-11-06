# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import abc
import copy
import importlib
import inspect
import random
import re
import time
import typing
import warnings
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import sqlalchemy as sa
import sqlalchemy.exc
from packaging.version import Version
from pandas.core.dtypes.base import ExtensionDtype
from sqlalchemy.sql.base import ColumnCollection

from pydiverse.common import Date, Dtype, PandasBackend
from pydiverse.common.util.computation_tracing import ComputationTracer
from pydiverse.common.util.hashing import stable_hash
from pydiverse.pipedag import ConfigContext, Stage
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
from pydiverse.pipedag.container import ExternalTableReference, Schema, SortCol, SortOrder, Table, View
from pydiverse.pipedag.context import TaskContext
from pydiverse.pipedag.errors import HookCheckException
from pydiverse.pipedag.materialize.details import resolve_materialization_details_label
from pydiverse.pipedag.materialize.materializing_task import MaterializingTask
from pydiverse.pipedag.materialize.table_hook_base import (
    AutoVersionSupport,
    CanMatResult,
    CanRetResult,
    TableHook,
)
from pydiverse.pipedag.optional_dependency.colspec import cs
from pydiverse.pipedag.optional_dependency.dataframely import dy
from pydiverse.pipedag.optional_dependency.ibis import ibis
from pydiverse.pipedag.optional_dependency.sqlalchemy import Select, SqlText, TextClause
from pydiverse.pipedag.optional_dependency.tidypolars import Tibble, tidypolars
from pydiverse.pipedag.optional_dependency.transform import ColExpr, Pandas, Polars, SqlAlchemy, pdt, pdt_new, pdt_old
from pydiverse.pipedag.util.sql import compile_sql


def _polars_apply_retrieve_annotation(df, table, store, intentionally_empty: bool = False):
    if cs.ColSpec is not object:
        if inspect.isclass(table.annotation) and issubclass(table.annotation, cs.ColSpec):
            column_spec = table.annotation  # type: cs.ColSpec
            if dy.Column is not None:
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
    if dy.Column is not None:
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
    if dy.Column is not None:
        column_spec = None
        # support dy.LazyFrame[T] and dy.DataFrame[T] annotations
        if typing.get_origin(table.annotation) is not None and issubclass(
            typing.get_origin(table.annotation), dy.LazyFrame | dy.DataFrame
        ):
            anno_args = typing.get_args(table.annotation)
            if len(anno_args) == 1:
                column_spec = anno_args[0]
        # but also support Schema annotations directly since there is no dy.PdtTable[T]
        if inspect.isclass(table.annotation) and issubclass(table.annotation, dy.Schema):
            column_spec = table.annotation
        if column_spec and issubclass(column_spec, dy.Schema):
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
    if cs.ColSpec is not object:
        if inspect.isclass(table.annotation) and issubclass(table.annotation, cs.ColSpec):
            column_spec = table.annotation
            if dy.Column is not None:
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
    if cs.ColSpec is not object:
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
        query = sa.text(str(tbl >> pdt.build_query()).replace("%%", "%"))  # undo %% quoting
        schema = store.write_subquery(query, table_name, neighbor_table=table, unlogged=unlogged, suffix=suffix)
        return pdt.Table(table_name, pdt.SqlAlchemy(store.engine, schema=schema.get()))

    def materialize_hook(tbl: pdt.Table, table_prefix: str | None):
        name = table_prefix or "_t_"
        name += stable_hash(str(random.randbytes(8)))[0:6]
        intermediate_tbls.append(name)
        return write_pdt_table(tbl, name)

    if cs.ColSpec is not object:
        if inspect.isclass(table.annotation) and issubclass(table.annotation, cs.ColSpec):
            column_spec = table.annotation
            if pdt_new is not None:
                supported_dialects = ["duckdb", "sqlite", "postgresql", "mssql", "ibm_db_sa"]
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


def _is_hide_errors():
    try:
        cfg = ConfigContext.get()
    except LookupError:
        return False
    # some tests use `with cfg.evolve(_swallow_exceptions=True)`
    return cfg._swallow_exceptions


def get_view_query(view: View, store: SQLTableStore):
    assert view.assert_normalized  # otherwise, view.src might be iterable for various reasons
    if isinstance(view.src, Iterable):
        src_tables = [
            SQLAlchemyTableHook.retrieve(store, src_tbl, src_tbl.stage.current_name, sa.Table) for src_tbl in view.src
        ]
        assert len(src_tables) > 0
        cols = src_tables[0].c  # this might be oversimplified for categorical columns
        src_select = [sa.select(sa.text("*")).select_from(alias.original) for alias in src_tables]
        base_from = sa.union_all(*src_select).alias("sub")

        # reconstruct columns outside of UNION (this pain yields nicer query due to select("*") above)
        def bind(c: sa.Column, expr):
            col = sa.Column(c.name, c.type)
            col.table = expr
            return col

        try:
            from sqlalchemy.sql.base import ReadOnlyColumnCollection

            base_from.c = ReadOnlyColumnCollection(ColumnCollection([(c.name, bind(c, base_from)) for c in cols]))
        except:  # noqa
            # support sqlalchemy < 2.0 and generally fall back to uglier query but more reliable code
            base_from = sa.union_all(*[sa.select(t) for t in src_tables]).alias("sub")
    else:
        base_from = SQLAlchemyTableHook.retrieve(store, view.src, view.src.stage.current_name, sa.Table)
        if not view.sort_by and not view.columns and not view.limit:
            # avoid returning subquery
            return base_from
    if view.columns:
        query = sa.select(*[base_from.c[col].label(name) for name, col in view.columns.items()]).select_from(base_from)
    else:
        query = sa.select(base_from)
    if view.sort_by and view.limit != 0:
        # for SQL, it is no problem to execute sort_by after columns because the source columns can still be referenced
        query = query.order_by(*[col.sql(base_from.c) for col in view.sort_by])
        # attention: the view.limit != 0 is important for mssql dialect because it cannot order by subqueries
        #   and we use subqueries for empty AUTO_VERSION pulls.
    if view.limit:
        query = query.limit(view.limit)
    return query


def get_view_query_pdt(view: View, store: SQLTableStore, name: str, stage_name: str, limit: int | None):
    assert view.assert_normalized
    if isinstance(view.src, Iterable):
        raise NotImplementedError(
            "View dematerialization with pydiverse.transform input_type and "
            "multiple source tables is not implemented yet."
        )
        # src_tables = [
        #     SQLAlchemyTableHook.retrieve(store, src_tbl, src_tbl.stage.current_name, sa.Table) for src_tbl in view.src
        # ]
        # assert len(src_tables) > 0
        # tbls = [
        #     pdt.Table(alias.original.name, SqlAlchemy(store.engine, schema=alias.original.schema), name=src_tbl.name)
        #     for alias, src_tbl in zip(src_tables, view.src)
        # ]
        # base_from = pdt.union_all(*tbls)
    hook = store.get_r_table_hook(sa.Table)
    sa_tbl = hook.retrieve(store, view.src, stage_name, sa.Table, limit=None)
    tbl = pdt.Table(sa_tbl.original.name, SqlAlchemy(store.engine, schema=sa_tbl.original.schema), name=name)

    def pdt_sort_col(tbl: pdt.Table, col: SortCol):
        if col.order == SortOrder.DESC:
            c = tbl[col.col].descending()
        else:
            c = tbl[col.col]
        if col.nulls_first:
            c = c.nulls_first()
        elif col.nulls_first == False:  # noqa: E712
            c = c.nulls_last()
        return c

    limit = min(limit, view.limit) if limit is not None and view.limit is not None else (view.limit or limit)
    if view.sort_by is not None and limit != 0:
        tbl = tbl >> pdt.arrange(*[pdt_sort_col(tbl, col) for col in view.sort_by])
        # atteintion: the view.limit != 0 is important for mssql dialect because it cannot order by subqueries
        #   and we use subqueries for empty AUTO_VERSION pulls.

    if view.columns is not None:
        tbl = tbl >> pdt.rename({v: k for k, v in view.columns.items()}) >> pdt.select(*view.columns.keys())

    if limit is not None:
        tbl = tbl >> pdt.slice_head(limit)

    return tbl


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
        try:
            cfg = ConfigContext.get()
        except LookupError:
            return ret
        if hook_args := cfg.table_hook_args.get("sql", None):
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
        return CanMatResult.new(issubclass(type_, (TextClause, sa.sql.expression.Selectable)))

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
        table: Table[TextClause | sa.sql.expression.Selectable],
        stage_name,
        without_config_context: bool = False,
    ):
        query = table.obj
        if isinstance(table.obj, (sa.Table, sa.sql.expression.Alias)):
            query = sa.select(sa.text("*")).select_from(table.obj)
            tbl = table.obj
            while hasattr(tbl, "original"):
                tbl = tbl.original
            if hasattr(tbl, "name"):
                source_tables = [
                    dict(
                        name=tbl.name,
                        schema=tbl.schema,
                        shared_lock_allowed=table.shared_lock_allowed,
                    )
                ]
            else:
                # This happens for views pointing to multiple parquet files (ParquetTableStore).
                # But this doesn't need locking of source tables anyways.
                source_tables = []
        else:
            try:
                input_tables = TaskContext.get().input_tables
            except LookupError:
                input_tables = []

            def get_name(tbl: Table):
                if tbl.view:
                    if isinstance(tbl.view.src, Iterable):
                        return list(tbl.view.src)[0].name
                    return tbl.view.src.name
                else:
                    return tbl.name

            def get_stage(tbl: Table):
                if tbl.view:
                    if isinstance(tbl.view.src, Iterable):
                        return list(tbl.view.src)[0].stage
                    else:
                        return tbl.view.src.stage
                else:
                    return tbl.stage

            source_tables = [
                dict(
                    name=get_name(tbl),
                    schema=store.get_schema(get_stage(tbl).current_name).get()
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
                    if _is_hide_errors():
                        log = store.logger.info
                    else:
                        log = store.logger.error
                    log(
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
        store.execute(
            cls._create_as_select_statements(
                table.name, schema, limit_query, store, suffix, unlogged, guaranteed_empty=True
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
        query: Select | TextClause | SqlText,
        store: SQLTableStore,
        suffix: str,
        unlogged: bool,
        guaranteed_empty: bool = False,
    ):
        # most dialects don't need to know whether CreateTableAsSelect executes an empty insert or not
        _ = store, guaranteed_empty
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
    def get_view_query(cls, view: View, store: SQLTableStore):
        """ParquetTableStore implements special version for src-only views."""
        return get_view_query(view, store)

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

        if table.view:
            return cls.get_view_query(table.view, store).alias(alias_name)
        tbl = store.reflect_table(table_name, schema)
        return tbl.alias(alias_name)

    @classmethod
    def lazy_query_str(cls, store, obj) -> str:
        if isinstance(obj, sa.sql.expression.FromClause):
            query = sa.select(sa.text("*")).select_from(obj)
        else:
            query = obj
        query_str = str(query.compile(store.engine, compile_kwargs={"literal_binds": True}))
        # hacky way to canonicalize query (despite __tmp/__even/__odd suffixes
        # and alias resolution)
        query_str = re.sub(r'["\[\]]', "", query_str)
        query_str = re.sub(r'(__tmp|__even|__odd)(?=[ \t\n.;"]|$)', "", query_str.lower())
        return query_str


@SQLTableStore.register_table()
class ExternalTableReferenceMaterializationHook(TableHook[SQLTableStore]):
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

    @classmethod
    def lazy_query_str(cls, store, obj: ExternalTableReference) -> str:
        assert isinstance(obj, ExternalTableReference)
        return str(obj)


class BaseViewMaterializationHook(TableHook[SQLTableStore], abc.ABC):
    @classmethod
    @abc.abstractmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        raise NotImplementedError

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        return CanRetResult.NO

    @classmethod
    def fuse_sub_views(cls, view: View, sub_view: View):
        """
        If a view is defined on top of another view, we fuse the two views into one.

        param in_out_table: The table with the view to be fused. It will be modified in place.
        """
        if hasattr(sub_view.src, "view") and sub_view.src.view:
            # recursive call
            sub_view = cls.fuse_sub_views(sub_view, sub_view.src.view)

        def check_missing(cols, sub_cols):
            cols = [col.col if isinstance(col, SortCol) else col for col in cols]
            if any(col not in sub_cols for col in cols):
                missing = [col for col in cols if col not in sub_cols]
                raise ValueError(f"Cannot fuse views because columns {missing} are not present in the sub view.")

        def translate_col(col, sub_cols):
            if isinstance(col, SortCol):
                return SortCol(sub_cols.get(col.col, col.col), col.order)
            else:
                return sub_cols.get(col, col)

        def translate(cols, sub_cols):
            if not isinstance(sub_cols, Mapping):
                return cols
            if cols is None:
                return None
            if isinstance(cols, Mapping):
                check_missing(cols.values(), sub_cols)
                return {name: translate_col(col, sub_cols) for name, col in cols.items()}
            if isinstance(cols, Iterable) and not isinstance(cols, str):
                check_missing(cols, sub_cols)
                return [translate_col(col, sub_cols) for col in cols]
            check_missing([cols], sub_cols)
            return translate_col(cols, sub_cols)

        return View(
            src=sub_view.src,
            sort_by=translate(view.sort_by, sub_view.columns) or sub_view.sort_by,
            columns=translate(view.columns, sub_view.columns) or sub_view.columns,
            limit=min(view.limit, sub_view.limit)
            if view.limit is not None and sub_view.limit is not None
            else view.limit or sub_view.limit,
        )

    @classmethod
    def is_iterable_table(cls, src: Any) -> bool:
        return False

    @classmethod
    def replace_view_src_tables(cls, src: Iterable[Any] | Any, input_table_mapping: dict[Any, Table]):
        # For Views, we check whether the source tables actually exist as references that
        # were going into the task. We then replace the sources with dag.Table objects.
        msg = (
            "View source table `View(src=...)` not found in input tables. Please make sure to put the "
            "exact table references as you received them as task inputs. "
        )
        if isinstance(src, Iterable) and not cls.is_iterable_table(src):
            # Lookup both the src and src.obj if it is a Table. This is necessary for auto-table classes.
            replaced_src = [
                input_table_mapping.get(id(src_tbl))
                or (input_table_mapping.get(id(src_tbl.obj)) if isinstance(src, Table) else None)
                for src_tbl in src
            ]
            if any(tbl is None for tbl in replaced_src):
                for src_tbl in src:
                    src_raw_tbl = input_table_mapping.get(id(src_tbl)) or (
                        input_table_mapping.get(id(src_tbl.obj)) if isinstance(src, Table) else None
                    )
                    if src_raw_tbl is None:
                        raise ValueError(msg + f"View source({type(src_tbl)}): '{src_tbl}'")
            replaced_src = [tbl for tbl, _ in replaced_src]  # only dag.Table is relevant in tuple result
            if len(replaced_src) == 0:
                raise ValueError(f"Empty iterable (type={type(src)}) is not allowed as view source `View(src=...)`")
            if any(src_tbl.view for src_tbl in replaced_src) and len(replaced_src) > 1:
                raise ValueError(f"View on view is only allowed with exactly one source: {replaced_src}")
            if len(replaced_src) == 1:
                return replaced_src[0]
            return replaced_src

        # Lookup both the src and src.obj if it is a Table. This is necessary for auto-table classes.
        src_raw_tbl = input_table_mapping.get(id(src)) or (
            input_table_mapping.get(id(src.obj)) if isinstance(src, Table) else None
        )
        if src_raw_tbl is None:
            raise ValueError(msg + f"View source({type(src)}): '{src}'")
        return src_raw_tbl[0]  # drop second tuple result

    @classmethod
    @abc.abstractmethod
    def get_column_name(cls, col: str):
        raise NotImplementedError  # abstract class method

    @classmethod
    def get_column_label(cls, col: str):
        return cls.get_column_name(col)

    @classmethod
    def get_column_order(cls, col: str | SortCol | Any):
        if isinstance(col, SortCol):
            return SortCol(cls.get_column_name(col.col), col.order)
        else:
            return SortCol(cls.get_column_name(col), SortOrder.ASC)

    @classmethod
    def update_column_references(cls, table: Table):
        """
        Update column references in the view to be string names.

        param in_out_table: The table with the view to be updated. It will be modified in place.
        """
        # Furthermore, we convert column references with string names given the input table types
        if isinstance(table.view.columns, Mapping):
            columns = {name: cls.get_column_name(col) for name, col in table.view.columns.items()}
        elif isinstance(table.view.columns, Iterable) and not isinstance(table.view.columns, str):
            columns = {cls.get_column_label(col): cls.get_column_name(col) for col in table.view.columns}
        elif table.view.columns is not None:
            columns = {cls.get_column_label(table.view.columns): cls.get_column_name(table.view.columns)}
        else:
            columns = None

        if isinstance(table.view.sort_by, Iterable) and not isinstance(table.view.sort_by, str):
            sort_by = [cls.get_column_order(col) for col in table.view.sort_by]
        elif table.view.sort_by is not None:
            sort_by = [cls.get_column_order(table.view.sort_by)]
        else:
            sort_by = None

        return columns, sort_by

    @classmethod
    def materialize(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        without_config_context: bool = False,
    ):
        input_table_mapping = TaskContext.get().input_table_mapping
        assert input_table_mapping is not None  # see materializing_task.py
        normalized_src = cls.replace_view_src_tables(table.view.src, input_table_mapping)
        columns, sort_by = cls.update_column_references(table)
        view = View(normalized_src, columns=columns, sort_by=sort_by, limit=table.view.limit)
        if hasattr(normalized_src, "view") and normalized_src.view is not None:
            # fuse views if the source is itself a view (we don't support multi-sources with views)
            view = cls.fuse_sub_views(view, view.src.view)
        # write back view to table so it is prepared for serialization and handing on to consumer tasks
        table.view = view.clone_assert_normalized()
        return

    @classmethod
    def retrieve(cls, *args, **kwargs):
        raise RuntimeError("This should never get called.")

    @classmethod
    def lazy_query_str(cls, store, obj: View) -> str:
        assert isinstance(obj, View)
        # we need to materialize the view to convert input_type dependent objects to serializable ones (mostly strings)
        dummy_tbl = Table(copy.copy(obj))
        dummy_tbl.stage = Stage("dummy")
        store.store_table(dummy_tbl, task=None)
        # the __repr__ function includes a comment that it should not contain any ID/address
        return str(dummy_tbl.view)


@SQLTableStore.register_table()
class ViewStrMaterializationHook(BaseViewMaterializationHook):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        all_columns_str = True
        if tbl.view and tbl.view.columns is not None:
            if isinstance(tbl.view.columns, Mapping):
                all_columns_str = all(isinstance(col, str) for col in tbl.view.columns.values())
            elif isinstance(tbl.view.columns, Iterable) and not isinstance(tbl.view.columns, str):
                all_columns_str = all(isinstance(col, str) for col in tbl.view.columns)
            else:
                all_columns_str = isinstance(tbl.view.columns, str)
        if tbl.view and tbl.view.sort_by is not None and all_columns_str:
            if isinstance(tbl.view.sort_by, Iterable) and not isinstance(tbl.view.sort_by, str):
                all_columns_str = all(isinstance(col, str) for col in tbl.view.sort_by)
            else:
                all_columns_str = isinstance(tbl.view.sort_by, str)
        return CanMatResult.new(tbl.view is not None and all_columns_str)

    @classmethod
    def get_column_name(cls, col: str):
        return col


@SQLTableStore.register_table()
class ViewSqlalchemyMaterializationHook(BaseViewMaterializationHook):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        try:
            task = TaskContext.get().task
        except LookupError:
            # This happens for SqlTableStore._copy_table() in background thread, but this will never
            # happen for materializing views (which is more preparing for serialization).
            return CanMatResult.NO
        if not isinstance(task, MaterializingTask):
            return CanMatResult.NO
        input_type = task._input_type
        return CanMatResult.YES_BUT_DONT_CACHE if tbl.view is not None and input_type == sa.Table else CanMatResult.NO

    @classmethod
    def get_column_label(cls, col: sa.Column | sa.sql.elements.Label | str):
        if isinstance(col, str):
            return col
        return col.name

    @classmethod
    def get_column_name(cls, col: sa.Column | sa.sql.elements.Label | str):
        if isinstance(col, str):
            return col
        if isinstance(col, sa.sql.elements.Label):
            return col.element.name
        return col.name

    @classmethod
    def get_column_order(cls, col: str | SortCol | sa.Column):
        if hasattr(col, "modifier"):
            if col.modifier.__name__ == "desc_op":
                return SortCol(cls.get_column_name(col.element), SortOrder.DESC)
            elif col.modifier.__name__ == "asc_op":
                return SortCol(cls.get_column_name(col.element), SortOrder.ASC)
            else:
                raise RuntimeError(
                    "Unsupported version of SqlAlchemy. "
                    "col.asc() / col.desc() should have modifier functions asc_op or desc_op. "
                    f"modifier={col.modifier.__name__}"
                )
        if isinstance(col, SortCol):
            return SortCol(cls.get_column_name(col.col), col.order)
        else:
            return SortCol(cls.get_column_name(col), SortOrder.ASC)


# endregion

# region PANDAS


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
    def dialect_supports_connectorx(cls):
        # ConnectorX (used by Polars read_database_uri) does not support many dialects
        return False

    @classmethod
    def dialect_wrong_polars_column_names(cls):
        # for Snowflake, polars returns uppercase column names by default
        return False

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

        if table.view:
            if limit is not None:
                if table.view.limit is not None:
                    table.view.limit = min(table.view.limit, limit)
                else:
                    table.view.limit = limit
                limit = None  # avoid additional subquery
                # attention: it is important for mssql dialect that limit=0 is applied in the view
            query = get_view_query(table.view, store).alias("tbl")
        else:
            query = store.reflect_table(table_name, schema).alias("tbl")

        def fix(t):
            if isinstance(t, sa.DECIMAL) and t.precision >= 19 and t.scale == 0:
                return sa.BigInteger()
            elif isinstance(t, sa.DECIMAL) and t.precision >= 10 and t.scale == 0:
                return sa.Integer()
            elif isinstance(t, sa.DECIMAL) and t.scale == 0:
                return sa.SmallInteger()
            elif str(t).startswith("TIMESTAMP"):
                # we don't think TIMEZONE based timestamps make sense in data exploration
                return sa.DateTime()
            else:
                return t

        cols = {col.name: col for col in query.columns}
        dtypes = {name: Dtype.from_sql(fix(col.type)) for name, col in cols.items()}

        cols, dtypes = cls._adjust_cols_retrieve(store, cols, dtypes)

        if cols is None:
            query = sa.select(sa.text("*")).select_from(query)
        else:
            query = sa.select(*cols.values()).select_from(query)
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
                # pandas hack for time64: convert via timestamp to avoid NA-only output
                if str(dtype).startswith("time64"):
                    df[col] = df[col].astype(str(dtype).replace("time64", "timestamp"))
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
        # The function _build_retrieve_query takes care of resolving views as input table.

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
        if table.name is not None:
            dataframe.attrs["name"] = table.name
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
        try:
            cfg = ConfigContext.get()
        except LookupError:
            return ret
        if hook_args := cfg.table_hook_args.get("polars", None):
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
                return cls.adbc_write_database(df, store, schema_name, table_name)
            except Exception as e:  # noqa
                store.logger.warning(
                    f"Failed writing table using ADBC, falling back to sqlalchemy: {table.name}",
                )
        df.write_database(
            f"{schema_name}.{table_name}",
            engine,
            if_table_exists="append",
        )

    @classmethod
    def adbc_read_database(cls, query, store: SQLTableStore) -> pl.DataFrame:
        connection_uri = store.engine_url.render_as_string(hide_password=False)
        df = pl.read_database_uri(query, connection_uri, engine="adbc")
        return df

    @classmethod
    def adbc_write_database(
        cls, df: pl.DataFrame, store: SQLTableStore, schema_name: str, table_name: str, if_table_exists="append"
    ) -> int:
        engine = store.engine
        return df.write_database(
            f"{schema_name}.{table_name}",
            engine.url.render_as_string(hide_password=False),
            if_table_exists=if_table_exists,
            engine="adbc",
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
        df = None
        if cls.dialect_has_adbc_driver():
            try:
                df = cls.adbc_read_database(query, store)
            except:  # noqa
                msg = "Failed retrieving query using ADBC, falling back to Pandas: %s"
                drivers = ["postgresql", "snowflake"]  # extend as needed
                for driver in drivers:
                    if store.engine.dialect.name == driver:
                        modname = f"adbc_driver_{driver}"
                        try:
                            importlib.import_module(modname)
                        except ImportError:
                            msg = (
                                f"Failed retrieving query using ADBC. Please install adbc-driver-{driver}. "
                                "Falling back to Pandas: %s"
                            )
                store.logger.warning(msg % query)
        if df is None and cls.dialect_supports_connectorx():
            # This implementation requires connectorx which does not work for duckdb or ibm_db2.
            # Attention: In case this call fails, we simply fall-back to pandas hook.
            try:
                df = pl.read_database_uri(query, connection_uri)
            except:  # noqa
                msg = "Failed retrieving query using ConnectorX, falling back to Pandas: %s"
                try:
                    import connectorx

                    _ = connectorx
                except ImportError:
                    msg = (
                        "Failed retrieving query using ConnectorX. Please install connectorx. "
                        "Falling back to Pandas: %s"
                    )
                store.logger.warning(msg, query)

        if df is None:
            # Polars internally falls back to pandas but does some magic around it
            df = pl.read_database(query, store.engine)

        # fix capital default column names
        if any(c.isupper() for c in df.columns) and cls.dialect_wrong_polars_column_names():
            with store.engine.connect() as conn:
                rs = conn.execute(sa.text(query) if isinstance(query, str) else query)
            df = df.rename({old: new for old, new in zip(df.columns, rs.keys())})

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
            if _is_hide_errors():
                log = store.logger.info
            else:
                log = store.logger.error
            log(
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
        # _execute_query and _build_retrieve_query take care of resolving views as input table.

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
        fallback_pandas = True
        if cls.dialect_supports_polars_native_read():
            try:
                df = cls.download_table(query, store, dtypes)
                fallback_pandas = False
            except (RuntimeError, ModuleNotFoundError) as e:
                store.logger.error(
                    "Fallback via Pandas since Polars failed to execute query on database %s: %s",
                    store.engine_url.render_as_string(hide_password=True),
                    e,
                )
        if fallback_pandas:
            pandas_hook = store.get_r_table_hook(pd.DataFrame)  # type: PandasTableHook
            backend = PandasBackend.ARROW
            query, dtypes = pandas_hook._build_retrieve_query(store, table, stage_name)
            dtypes = {name: dtype.to_pandas(backend) for name, dtype in dtypes.items()}
            pd_df = pandas_hook.download_table(query, store, dtypes)
            df = pl.from_pandas(pd_df)
        return df

    @classmethod
    def dialect_supports_polars_native_read(cls):
        # for most dialects we find a way
        return True


@SQLTableStore.register_table(pl)
class LazyPolarsTableHook(TableHook[SQLTableStore]):
    auto_version_support = AutoVersionSupport.LAZY

    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        return CanMatResult.new(type_ == pl.LazyFrame)

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        if dy.Column is not None and issubclass(type_, dy.LazyFrame):
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
        # polars_hook will take care of resolving views as input table.

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
        # polars_hook will take care of resolving views as input table.

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
        # polars_hook will take care of resolving views as input table.
        polars_hook = store.get_r_table_hook(pl.DataFrame)
        df = polars_hook.retrieve(store, table, stage_name, as_type, limit)
        return tidypolars.from_polars(df)

    @classmethod
    def auto_table(cls, obj: Tibble):
        # currently, we don't know how to store a table name inside tidypolar   s tibble
        return super().auto_table(obj)


# endregion

# region PYDIVERSE TRANSFORM


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
        # hooks responsible for pd.DataFrame and sa.Table take care of resolving views as input table.
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
    auto_version_support = AutoVersionSupport.LAZY

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
            table_cpy.obj = sa.text(str(query).replace("%%", "%"))  # undo %% quoting
            hook = store.get_m_table_hook(table_cpy)
            assert hook, "fatal error: no hook for materialization of SqlAlchemy query found"
            cfg = hook.cfg()
            if not cfg.disable_materialize_annotation_action:
                schema = store.get_schema(stage_name)
                t = _sql_apply_materialize_annotation_pdt_early(table, schema, store)
                table_cpy.obj = sa.text(str(t >> build_query()).replace("%%", "%"))
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
        # For input_type=SqlAlchemy, views are resolved by get_view_query_pdt. Hooks for
        # pl.LazyFrame and pd.DataFrame already take care of resolving views as input table.
        if issubclass(as_type, Polars):
            import polars as pl

            hook = store.get_r_table_hook(pl.LazyFrame)
            lf = hook.retrieve(store, table, stage_name, pl.DataFrame, limit)
            return pdt.Table(lf, name=table.name)
        elif issubclass(as_type, SqlAlchemy):
            if table.view is not None:
                return get_view_query_pdt(table.view, store, table.name, stage_name, limit)
            else:
                hook = store.get_r_table_hook(sa.Table)
                sa_tbl = hook.retrieve(store, table, stage_name, sa.Table, limit)
                return pdt.Table(
                    sa_tbl.original.name, SqlAlchemy(store.engine, schema=sa_tbl.original.schema), name=table.name
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

    @classmethod
    def retrieve_for_auto_versioning_lazy(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type[pdt.Table],
    ) -> pdt.Table:
        # polars_hook will take care of resolving views as input table.
        polars_hook = store.get_r_table_hook(pl.LazyFrame)  # type: PolarsTableHook
        lf = polars_hook.retrieve_for_auto_versioning_lazy(store, table, stage_name, as_type)
        return pdt.Table(lf, name=table.name)

    @classmethod
    def get_auto_version_lazy(cls, obj) -> str:
        from pydiverse.transform.extended import (
            build_query,
        )

        query = obj >> build_query()

        if query is not None:
            # This is intended to throw a TypeError because version=AUTO_VERSION
            # does not make sense for SQL tasks; use lazy=True instead
            return super().get_auto_version_lazy(obj)

        return LazyPolarsTableHook.get_auto_version_lazy(obj >> pdt.export(pdt.Polars(lazy=True)))


@SQLTableStore.register_table(pdt_new)
class ViewPdtMaterializationHook(BaseViewMaterializationHook):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        try:
            task = TaskContext.get().task
        except LookupError:
            # This happens for SqlTableStore._copy_table() in background thread, but this will never
            # happen for materializing views (which is more preparing for serialization).
            return CanMatResult.NO
        if not isinstance(task, MaterializingTask):
            return CanMatResult.NO
        input_type = task._input_type
        return (
            CanMatResult.YES_BUT_DONT_CACHE
            if tbl.view is not None and issubclass(input_type, (Polars, SqlAlchemy, Pandas))
            else CanMatResult.NO
        )

    @classmethod
    def is_iterable_table(cls, src: Any) -> bool:
        # pydiverse transform tables are iterable thus the detection of multiple source tables must be adjusted
        return isinstance(src, pdt.Table)

    @classmethod
    def get_column_name(cls, col: ColExpr | str):
        if isinstance(col, str):
            return col
        # use pydiverse.transform iternals to get underlying Column of expressions like col.ascending()
        from pydiverse.transform._internal.tree.col_expr import ColFn

        while isinstance(col, ColFn):
            col = next(col.iter_children())
        return col.name

    @classmethod
    def get_column_order(cls, col: str | SortCol | ColExpr):
        if isinstance(col, SortCol):
            return SortCol(cls.get_column_name(col.col), col.order, col.nulls_first)
        else:
            order, nulls_first = cls.extract_sort_opts(col)
            return SortCol(cls.get_column_name(col), order, nulls_first)

    @staticmethod
    def extract_sort_opts(col: str | ColExpr) -> tuple[SortOrder, bool]:
        # use pydiverse.transform iternals to figure out descending()/ascending() or nulls_first()/nulls_last()
        from pydiverse.transform._internal.tree.col_expr import ColFn

        order = None
        nulls_first = None
        col_it = col
        while isinstance(col_it, ColFn):
            if col_it.op.name == "descending":
                order = SortOrder.DESC
            elif col_it.op.name == "ascending":
                order = SortOrder.ASC
            elif col_it.op.name == "nulls_first":
                nulls_first = True
            elif col_it.op.name == "nulls_last":
                nulls_first = False
            col_it = next(col_it.iter_children())
        return order, nulls_first


# endregion

# region IBIS


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
        if table.view:
            raise NotImplementedError("IbisTableHook does not support retrieving views, yet.")
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
