from __future__ import annotations

import datetime
import re
import time
import warnings
from typing import Any

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc
import structlog
from packaging.version import Version

from pydiverse.pipedag import ConfigContext
from pydiverse.pipedag._typing import T
from pydiverse.pipedag.backend.table.base import AutoVersionSupport, TableHook
from pydiverse.pipedag.backend.table.sql.ddl import (
    CreateTableAsSelect,
    InsertIntoSelect,
)
from pydiverse.pipedag.backend.table.sql.sql import (
    SQLTableStore,
)
from pydiverse.pipedag.backend.table.util import (
    DType,
    PandasDTypeBackend,
)
from pydiverse.pipedag.container import ExternalTableReference, Schema, Table
from pydiverse.pipedag.context import TaskContext
from pydiverse.pipedag.materialize.details import resolve_materialization_details_label
from pydiverse.pipedag.util.computation_tracing import ComputationTracer

# region SQLALCHEMY


@SQLTableStore.register_table()
class SQLAlchemyTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(
            type_, (sa.sql.expression.TextClause, sa.sql.expression.Selectable)
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
        obj = table.obj
        if isinstance(table.obj, (sa.Table, sa.sql.expression.Alias)):
            obj = sa.select("*").select_from(table.obj)
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
            limit_query = store.get_limit_query(obj, rows=0)
            store.execute(
                CreateTableAsSelect(
                    table.name,
                    schema,
                    limit_query,
                    unlogged=unlogged,
                    suffix=suffix,
                )
            )
            store.add_indexes_and_set_nullable(table, schema, on_empty_table=True)
            statements = store.lock_table(table, schema)
            statements += store.lock_source_tables(source_tables)
            statements += [
                InsertIntoSelect(
                    table.name,
                    schema,
                    obj,
                )
            ]
            store.execute(
                statements,
                truncate_printed_select=True,
            )
            store.add_indexes_and_set_nullable(table, schema, on_empty_table=False)
        else:
            statements = store.lock_source_tables(source_tables)
            statements += [
                CreateTableAsSelect(
                    table.name,
                    schema,
                    obj,
                    unlogged=unlogged,
                    suffix=suffix,
                )
            ]
            store.execute(statements)
            store.add_indexes_and_set_nullable(table, schema)
        store.optional_pause_for_db_transactionality("table_create")

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
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, ExternalTableReference)

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
        dtypes: dict[str, DType],
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
        cls, query: Any, conn: sa.Connection, dtypes: dict[str, DType] | None = None
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
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pd.DataFrame)

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
        dtypes: dict[str:DType] | None,
        store: SQLTableStore,
        table: Table[Any],
        schema: Schema,
    ):
        """Helper function that can be invoked by other hooks"""
        if dtypes is None:
            dtypes = {
                name: DType.from_pandas(dtype) for name, dtype in df.dtypes.items()
            }

        for col, dtype in dtypes.items():
            # Currently, pandas' .to_sql fails for arrow date columns.
            # -> Temporarily convert all dates to objects
            # See: https://github.com/pandas-dev/pandas/issues/53854
            # TODO: Remove this once pandas 2.1 gets released (fixed by #53856)
            if dtype == DType.DATE:
                df[col] = df[col].astype(object)

        cls._execute_materialize(
            df,
            store=store,
            table=table,
            schema=schema,
            dtypes=dtypes,
        )

    @classmethod
    def _get_dialect_dtypes(cls, dtypes: dict[str, DType], table: Table[pd.DataFrame]):
        _ = table
        return {name: dtype.to_sql() for name, dtype in dtypes.items()}

    @classmethod
    def _dialect_create_empty_table(
        cls,
        store: SQLTableStore,
        df: pd.DataFrame,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, DType],
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
        dtypes: dict[str, DType],
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

        backend = PandasDTypeBackend(backend_str)

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
        backend: PandasDTypeBackend,
    ) -> tuple[Any, dict[str, DType]]:
        table_name, schema = store.resolve_alias(table, stage_name)

        sql_table = store.reflect_table(table_name, schema).alias("tbl")

        cols = {col.name: col for col in sql_table.columns}
        dtypes = {name: DType.from_sql(col.type) for name, col in cols.items()}

        cols, dtypes = cls._adjust_cols_retrieve(cols, dtypes, backend)

        query = sa.select(*cols.values()).select_from(sql_table)
        return query, dtypes

    @classmethod
    def _adjust_cols_retrieve(
        cls, cols: dict, dtypes: dict, backend: PandasDTypeBackend
    ) -> tuple[dict, dict]:
        if backend == PandasDTypeBackend.ARROW:
            return cols, dtypes

        assert backend == PandasDTypeBackend.NUMPY

        # Pandas datetime64[ns] can represent dates between 1678 AD - 2262 AD.
        # As such, when reading dates from a database, we must ensure that those
        # dates don't overflow the range of representable dates by pandas.
        # This is done by clipping the date to a predefined range and adding a
        # years column.

        res_cols = cols.copy()
        res_dtypes = dtypes.copy()

        for name, col in cols.items():
            if isinstance(col.type, (sa.Date, sa.DateTime)):
                if isinstance(col.type, sa.Date):
                    min_val = datetime.date(1700, 1, 1)
                    max_val = datetime.date(2200, 1, 1)
                elif isinstance(col.type, sa.DateTime):
                    min_val = datetime.datetime(1700, 1, 1, 0, 0, 0)
                    max_val = datetime.datetime(2200, 1, 1, 0, 0, 0)
                else:
                    raise

                # Year column
                year_col_name = f"{name}_year"
                if year_col_name not in cols:
                    year_col = sa.cast(sa.func.extract("year", col), sa.Integer)
                    year_col = year_col.label(year_col_name)
                    res_cols[year_col_name] = year_col
                    res_dtypes[year_col_name] = DType.INT16

                # Clamp date range
                clamped_col = sa.case(
                    (col.is_(None), None),
                    (col < min_val, min_val),
                    (col > max_val, max_val),
                    else_=col,
                ).label(name)
                res_cols[name] = clamped_col

        return res_cols, res_dtypes

    @classmethod
    def _execute_query_retrieve(
        cls,
        store: SQLTableStore,
        query: Any,
        dtypes: dict[str, DType],
        backend: PandasDTypeBackend,
    ) -> pd.DataFrame:
        dtypes = {name: dtype.to_pandas(backend) for name, dtype in dtypes.items()}

        with store.engine.connect() as conn:
            return cls.download_table(query, conn, dtypes)

    # Auto Version

    class ComputationTracer(ComputationTracer):
        def _monkey_patch(self):
            import numpy
            import pandas

            from pydiverse.pipedag.util.computation_tracing import patch

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
    def download_table(cls, query: Any, connection_uri: str) -> polars.DataFrame:
        """
        Provide hook that allows to override the default
        download of polars tables from the tablestore.
        """
        # TODO: consider using arrow_odbc or adbc-driver-postgresql together with:
        # Cursor.fetchallarrow()

        # This implementation requires connectorx which does not work for duckdb
        # and osx-arm64.
        # Attention: In case this call fails, we simply fall-back to pandas hook.
        df = polars.read_database_uri(query, connection_uri)
        return df

    @classmethod
    def can_materialize(cls, type_) -> bool:
        # attention: tidypolars.Tibble is subclass of polars DataFrame
        return type_ == polars.DataFrame

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

        dtypes = dict(zip(df.columns, map(DType.from_polars, df.dtypes)))

        pd_df = df.to_pandas(use_pyarrow_extension_array=True, zero_copy_only=True)
        pandas_hook = store.get_hook_subclass(PandasTableHook)
        return pandas_hook.materialize_(
            df=pd_df,
            dtypes=dtypes,
            store=store,
            table=table,
            schema=schema,
        )

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
            return cls._execute_query(query, connection_uri, store)
        except (RuntimeError, ModuleNotFoundError) as e:
            logger = structlog.get_logger(logger_name=cls.__name__)
            logger.error(
                "Fallback via Pandas since Polars failed to execute query on "
                "database %s: %s",
                store.engine_url.render_as_string(hide_password=True),
                e,
            )
            pandas_hook = store.get_hook_subclass(PandasTableHook)
            with store.engine.connect() as conn:
                pd_df = pandas_hook.download_table(query, conn)
            return polars.from_pandas(pd_df)

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
            df = cls.download_table(query, connection_uri)
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
            pandas_hook = store.get_hook_subclass(PandasTableHook)
            with engine.connect() as conn:
                pd_df = pandas_hook.download_table(query, conn)
            engine.dispose()
            return polars.from_pandas(pd_df)


@SQLTableStore.register_table(polars)
class LazyPolarsTableHook(TableHook[SQLTableStore]):
    auto_version_support = AutoVersionSupport.LAZY

    @classmethod
    def can_materialize(cls, type_) -> bool:
        return type_ == polars.LazyFrame

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == polars.LazyFrame

    @classmethod
    def materialize(cls, store, table: Table[polars.LazyFrame], stage_name):
        t = table.obj
        table = table.copy_without_obj()
        table.obj = t.collect()

        polars_hook = store.get_hook_subclass(PolarsTableHook)
        return polars_hook.materialize(store, table, stage_name)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[polars.DataFrame],
    ) -> polars.LazyFrame:
        polars_hook = store.get_hook_subclass(PolarsTableHook)
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
        polars_hook = store.get_hook_subclass(PolarsTableHook)

        # Retrieve with LIMIT 0 -> only get schema but no data
        query = polars_hook._read_db_query(store, table, stage_name)
        query = query.where(sa.false())  # LIMIT 0
        query = polars_hook._compile_query(store, query)
        connection_uri = store.engine_url.render_as_string(hide_password=False)

        df = polars_hook._execute_query(query, connection_uri, store)

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
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    tidypolars = None


@SQLTableStore.register_table(tidypolars, polars)
class TidyPolarsTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, tidypolars.Tibble)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == tidypolars.Tibble

    @classmethod
    def materialize(cls, store, table: Table[tidypolars.Tibble], stage_name):
        t = table.obj
        table = table.copy_without_obj()
        table.obj = t.to_polars()

        polars_hook = store.get_hook_subclass(PolarsTableHook)
        return polars_hook.materialize(store, table, stage_name)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[tidypolars.Tibble],
    ) -> tidypolars.Tibble:
        polars_hook = store.get_hook_subclass(PolarsTableHook)
        df = polars_hook.retrieve(store, table, stage_name, as_type)
        return tidypolars.from_polars(df)

    @classmethod
    def auto_table(cls, obj: tidypolars.Tibble):
        # currently, we don't know how to store a table name inside tidypolars tibble
        return super().auto_table(obj)


# endregion

# region PYDIVERSE TRANSFORM


try:
    # optional dependency to pydiverse-transform
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
            hook = store.get_hook_subclass(PandasTableHook)
            return hook.materialize(store, table, stage_name)
        if isinstance(t._impl, SQLTableImpl):
            table.obj = t._impl.build_select()
            hook = store.get_hook_subclass(SQLAlchemyTableHook)
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
            hook = store.get_hook_subclass(PandasTableHook)
            df = hook.retrieve(store, table, stage_name, pd.DataFrame)
            return pdt.Table(PandasTableImpl(table.name, df))
        if issubclass(as_type, SQLTableImpl):
            hook = store.get_hook_subclass(SQLAlchemyTableHook)
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


# endregion

# region IBIS


try:
    import ibis
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    ibis = None


@SQLTableStore.register_table(ibis)
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
    def can_materialize(cls, type_) -> bool:
        # Operations on a table like mutate() or join() don't change the type
        return issubclass(type_, ibis.api.Table)

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

        sa_hook = store.get_hook_subclass(SQLAlchemyTableHook)
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
                    schema=schema,
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
