from __future__ import annotations

import datetime
import re
import time
import warnings
from typing import Any

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc
from packaging.version import Version

from pydiverse.pipedag import ConfigContext
from pydiverse.pipedag._typing import T
from pydiverse.pipedag.backend.table.base import TableHook
from pydiverse.pipedag.backend.table.sql.ddl import (
    CreateTableAsSelect,
    Schema,
)
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore, TableReference
from pydiverse.pipedag.backend.table.util import (
    DType,
    PandasDTypeBackend,
)
from pydiverse.pipedag.context import TaskContext
from pydiverse.pipedag.materialize import Table
from pydiverse.pipedag.materialize.core import TaskInfo

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
    def materialize(
        cls,
        store,
        table: Table[sa.sql.expression.TextClause | sa.Text],
        stage_name,
        task_info: TaskInfo | None,
    ):
        obj = table.obj
        if isinstance(table.obj, (sa.Table, sa.sql.expression.Alias)):
            obj = sa.select("*").select_from(table.obj)

        source_tables = [
            dict(
                name=tbl.name,
                schema=store.get_schema(
                    tbl.stage.transaction_name
                    if tbl.stage in task_info.open_stages
                    else tbl.stage.name
                ).get(),
            )
            for tbl in task_info.input_tables
        ]
        schema = store.get_schema(stage_name)
        store.execute(
            CreateTableAsSelect(
                table.name,
                schema,
                obj,
                early_not_null=table.primary_key,
                source_tables=source_tables,
            )
        )
        store.add_indexes(table, schema, early_not_null_possible=True)

    @classmethod
    def retrieve(
        cls, store, table, stage_name, as_type: type[sa.Table]
    ) -> sa.sql.expression.Selectable:
        schema = store.get_schema(stage_name).get()
        table_name, schema = store.resolve_alias(table.name, schema)
        alias_name = TaskContext.get().name_disambiguator.get_name(table_name)

        for retry_iteration in range(4):
            # retry operation since it might have been terminated as a deadlock victim
            try:
                return sa.Table(
                    table_name,
                    sa.MetaData(),
                    schema=schema,
                    autoload_with=store.engine,
                ).alias(alias_name)
            except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError):
                if retry_iteration == 3:
                    raise
                time.sleep(retry_iteration * retry_iteration * 1.2)

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
class TableReferenceHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, TableReference)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return False

    @classmethod
    def materialize(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name,
        task_info: TaskInfo | None,
    ):
        # For a table reference, we don't need to materialize anything.
        # This is any table referenced by a table reference should already exist
        # in the schema.
        # Instead, we check that the table actually exists.
        schema = store.get_schema(stage_name).get()

        inspector = sa.inspect(store.engine)
        has_table = inspector.has_table(table.name, schema)

        if not has_table:
            raise ValueError(
                f"Not table with name '{table.name}' found in schema '{schema}' "
                "(reference by TableReference)."
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
        cls,
        store: SQLTableStore,
        table: Table[pd.DataFrame],
        stage_name,
        task_info: TaskInfo | None,
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
        store.add_indexes(table, schema)

    @classmethod
    def _execute_materialize(
        cls,
        df: pd.DataFrame,
        store: SQLTableStore,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, DType],
    ):
        dtypes = {name: dtype.to_sql() for name, dtype in dtypes.items()}
        if table.type_map:
            dtypes.update(table.type_map)

        df.to_sql(
            table.name,
            store.engine,
            schema=schema.get(),
            index=False,
            dtype=dtypes,
            chunksize=100_000,
        )

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type[pd.DataFrame] | tuple | dict,
    ) -> pd.DataFrame:
        # Config
        if PandasTableHook.pd_version >= Version("2.0"):
            # Once arrow is mature enough, we might want to switch to
            # arrow backed dataframes by default
            backend_str = "numpy"
        else:
            backend_str = "numpy"

        if hook_args := ConfigContext.get().table_hook_args.get("pandas", None):
            if dtype_backend := hook_args.get("dtype_backend", None):
                backend_str = dtype_backend

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
        stage_name: str,
        backend: PandasDTypeBackend,
    ) -> tuple[Any, dict[str, DType]]:
        engine = store.engine
        schema = store.get_schema(stage_name).get()
        table_name, schema = store.resolve_alias(table.name, schema)

        sql_table = sa.Table(
            table_name,
            sa.MetaData(),
            schema=schema,
            autoload_with=engine,
        ).alias("tbl")

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
        dtypes = {
            name: dtype.to_pandas(backend=backend) for name, dtype in dtypes.items()
        }

        with store.engine.connect() as conn:
            if PandasTableHook.pd_version >= Version("2.0"):
                df = pd.read_sql(query, con=conn, dtype=dtypes)
            else:
                df = pd.read_sql(query, con=conn)
                for col, dtype in dtypes.items():
                    df[col] = df[col].astype(dtype)

        return df


# endregion

# region POLARS


try:
    import connectorx
    import polars
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    polars = None
    connectorx = None


@SQLTableStore.register_table(polars, connectorx)
class PolarsTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        # attention: tidypolars.Tibble is subclass of polars DataFrame
        return type_ == polars.dataframe.DataFrame

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == polars.dataframe.DataFrame

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[polars.dataframe.DataFrame],
        stage_name,
        task_info: TaskInfo | None,
    ):
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
        hook = store.get_hook_subclass(PandasTableHook)
        return hook.materialize_(
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
        stage_name: str,
        as_type: type[polars.dataframe.DataFrame],
    ) -> polars.dataframe.DataFrame:
        schema = store.get_schema(stage_name).get()
        table_name, schema = store.resolve_alias(table.name, schema)
        connection_uri = store.engine_url.render_as_string(hide_password=False)

        preparer = store.engine.dialect.identifier_preparer
        q_table = preparer.quote(table_name)
        q_schema = preparer.format_schema(schema)

        df = polars.read_database(f"SELECT * FROM {q_schema}.{q_table}", connection_uri)
        return df

    @classmethod
    def auto_table(cls, obj: polars.dataframe.DataFrame):
        # currently, we don't know how to store a table name inside polars dataframe
        return super().auto_table(obj)


try:
    import tidypolars
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    tidypolars = None


@SQLTableStore.register_table(tidypolars, polars, connectorx)
class TidyPolarsTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, tidypolars.Tibble)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == tidypolars.Tibble

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[tidypolars.Tibble],
        stage_name,
        task_info: TaskInfo | None,
    ):
        t = table.obj
        table = table.copy_without_obj()
        table.obj = t.to_polars()

        hook = store.get_hook_subclass(PolarsTableHook)
        return hook.materialize(store, table, stage_name, task_info)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type[tidypolars.Tibble],
    ) -> tidypolars.Tibble:
        hook = store.get_hook_subclass(PolarsTableHook)
        df = hook.retrieve(store, table, stage_name, as_type)
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
    def materialize(
        cls, store, table: Table[pdt.Table], stage_name, task_info: TaskInfo | None
    ):
        from pydiverse.transform.core.verbs import collect
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        t = table.obj
        table = table.copy_without_obj()
        if isinstance(t._impl, PandasTableImpl):
            table.obj = t >> collect()
            hook = store.get_hook_subclass(PandasTableHook)
            return hook.materialize(store, table, stage_name, task_info)
        if isinstance(t._impl, SQLTableImpl):
            table.obj = t._impl.build_select()
            hook = store.get_hook_subclass(SQLAlchemyTableHook)
            return hook.materialize(store, table, stage_name, task_info)
        raise NotImplementedError

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
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
    def materialize(
        cls,
        store,
        table: Table[ibis.api.Table],
        stage_name,
        task_info: TaskInfo | None,
    ):
        t = table.obj
        table = table.copy_without_obj()
        table.obj = sa.text(cls.lazy_query_str(store, t))

        sa_hook = store.get_hook_subclass(SQLAlchemyTableHook)
        return sa_hook.materialize(store, table, stage_name, task_info)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type[ibis.api.Table],
    ) -> ibis.api.Table:
        conn = cls.conn(store)
        schema = store.get_schema(stage_name).get()
        table_name, schema = store.resolve_alias(table.name, schema)
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
