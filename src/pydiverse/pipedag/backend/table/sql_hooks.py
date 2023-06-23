from __future__ import annotations

import datetime
import re
import time
import warnings
from typing import Any

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.dialects
from packaging.version import Version

from pydiverse.pipedag._typing import T
from pydiverse.pipedag.backend.table.base import TableHook
from pydiverse.pipedag.backend.table.sql import SQLTableStore
from pydiverse.pipedag.backend.table.util import (
    DType,
    classmethod_engine_argument_dispatch,
)
from pydiverse.pipedag.backend.table.util.sql_ddl import (
    CreateTableAsSelect,
    Schema,
    ibm_db_sa_fix_name,
)
from pydiverse.pipedag.materialize import Table
from pydiverse.pipedag.materialize.core import TaskInfo
from pydiverse.pipedag.util.naming import NameDisambiguator

# region SQLALCHEMY


@SQLTableStore.register_table()
class SQLAlchemyTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(
            type_, (sa.sql.elements.TextClause, sa.sql.selectable.Selectable)
        )

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == sa.Table

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[sa.sql.elements.TextClause | sa.Text],
        stage_name,
        task_info: TaskInfo | None,
    ):
        obj = table.obj
        if isinstance(table.obj, (sa.Table, sa.sql.selectable.Alias)):
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
        cls, store, table, stage_name, as_type: type[sa.Table], namer=None
    ) -> sa.sql.selectable.Selectable:
        table_name = table.name
        schema = store.get_schema(stage_name).get()
        table_name, schema = store.resolve_aliases(table_name, schema)
        tbl = None
        for retry_iteration in range(4):
            # retry operation since it might have been terminated as a deadlock victim
            try:
                alias_name = (
                    namer.get_name(table_name) if namer is not None else table_name
                )
                tbl = sa.Table(
                    table_name,
                    sa.MetaData(),
                    schema=schema,
                    autoload_with=store.engine,
                ).alias(alias_name)
                break
            except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError):
                if retry_iteration == 3:
                    raise
                time.sleep(retry_iteration * retry_iteration * 1.2)
        return tbl

    @classmethod
    def lazy_query_str(cls, store, obj) -> str:
        if isinstance(obj, sa.sql.selectable.FromClause):
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


# endregion

# region PANDAS


@SQLTableStore.register_table(pd)
class PandasTableHook(TableHook[SQLTableStore]):
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

        dtypes = {name: DType.from_pandas(dtype) for name, dtype in df.dtypes.items()}
        cls._execute_materialize(
            table=table,
            schema=schema,
            engine=store.engine,
            dtypes=dtypes,
        )
        store.add_indexes(table, schema)

    @classmethod_engine_argument_dispatch
    def _execute_materialize(
        cls,
        table: Table[pd.DataFrame],
        schema: Schema,
        engine: sa.Engine,
        dtypes: dict[str, DType],
    ):
        dtypes = {name: dtype.to_sql() for name, dtype in dtypes.items()}
        if table.type_map:
            dtypes.update(table.type_map)

        df = table.obj
        df.to_sql(
            table.name,
            engine,
            schema=schema.get(),
            index=False,
            dtype=dtypes,
            chunksize=100_000,
        )

    @_execute_materialize.dialect("postgresql")
    def _execute_materialize_postgres(
        cls,
        table: Table[pd.DataFrame],
        schema: Schema,
        engine: sa.Engine,
        dtypes: dict[str, DType],
    ):
        import csv
        from io import StringIO

        dtypes = {name: dtype.to_sql() for name, dtype in dtypes.items()}
        if table.type_map:
            dtypes.update(table.type_map)

        df = table.obj

        # Create empty table
        df[:0].to_sql(
            table.name,
            engine,
            schema=schema.get(),
            index=False,
            dtype=dtypes,
        )

        # COPY data
        # TODO: For python 3.12, there is csv.QUOTE_STRINGS
        #       This would make everything a bit safer, because then we could represent
        #       the string "\\N" (backslash + capital n).
        s_buf = StringIO()
        df.to_csv(
            s_buf,
            na_rep="\\N",
            header=False,
            index=False,
            quoting=csv.QUOTE_MINIMAL,
        )
        s_buf.seek(0)

        dbapi_conn = engine.raw_connection()
        try:
            with dbapi_conn.cursor() as cur:
                sql = (
                    f"COPY {schema.get()}.{table.name} FROM STDIN"
                    " WITH (FORMAT CSV, NULL '\\N')"
                )
                cur.copy_expert(sql=sql, file=s_buf)
            dbapi_conn.commit()
        finally:
            dbapi_conn.close()

    @_execute_materialize.dialect("mssql")
    def _execute_materialize_mssql(
        cls,
        table: Table[pd.DataFrame],
        schema: Schema,
        engine: sa.Engine,
        dtypes: dict[str, DType],
    ):
        dtypes = ({name: dtype.to_sql() for name, dtype in dtypes.items()}) | (
            {
                name: sa.dialects.mssql.DATETIME2()
                for name, dtype in dtypes.items()
                if dtype == DType.DATETIME
            }
        )

        if table.type_map:
            dtypes.update(table.type_map)

        df = table.obj
        df.to_sql(
            table.name,
            engine,
            schema=schema.get(),
            index=False,
            dtype=dtypes,
            chunksize=100_000,
        )

    @_execute_materialize.dialect("ibm_db_sa")
    def _execute_materialize_mssql(
        cls,
        table: Table[pd.DataFrame],
        schema: Schema,
        engine: sa.Engine,
        dtypes: dict[str, DType],
    ):
        # Default string target is CLOB which can't be used for indexing.
        # -> Convert indexed string columns to VARCHAR(256)
        index_columns = set()
        if indexes := table.indexes:
            index_columns |= {col for index in indexes for col in index}
        if primary_key := table.primary_key:
            index_columns |= set(primary_key)

        dtypes = ({name: dtype.to_sql() for name, dtype in dtypes.items()}) | (
            {
                name: sa.String(length=256) if name in index_columns else sa.CLOB()
                for name, dtype in dtypes.items()
                if dtype == DType.STRING
            }
        )

        if table.type_map:
            dtypes.update(table.type_map)

        df = table.obj
        df.to_sql(
            ibm_db_sa_fix_name(table.name),
            engine,
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
        as_type: type[pd.DataFrame],
        namer: NameDisambiguator | None = None,
    ) -> pd.DataFrame:
        query, dtypes = cls._build_retrieve_query(store, table, stage_name)
        dataframe = cls._execute_query_retrieve(query, dtypes, store.engine)
        return dataframe

    @classmethod
    def _build_retrieve_query(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
    ) -> tuple[Any, dict[str, DType]]:
        engine = store.engine
        schema = store.get_schema(stage_name).get()
        table_name = table.name
        table_name, schema = store.resolve_aliases(table_name, schema)

        sql_table = sa.Table(
            table_name,
            sa.MetaData(),
            schema=schema,
            autoload_with=engine,
        ).alias("tbl")

        cols = {col.name: col for col in sql_table.columns}
        dtypes = {name: DType.from_sql(col.type) for name, col in cols.items()}

        cols, dtypes = cls._adjust_cols_retrieve(cols, dtypes, engine)

        query = sa.select(*cols.values()).select_from(sql_table)
        return query, dtypes

    @classmethod
    def _adjust_cols_retrieve(
        cls, cols: dict, dtypes: dict, engine: sa.Engine
    ) -> tuple[dict, dict]:
        res_cols = cols.copy()
        res_dtypes = dtypes.copy()

        for name, col in cols.items():
            # Pandas datetime64[ns] can represent dates between 1678 AD - 2262 AD.
            # As such, when reading dates from a database, we must ensure that those
            # dates don't overflow the range of representable dates by pandas.
            # This is done by clipping the date to a predefined range and adding a
            # years column.
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

    @classmethod_engine_argument_dispatch
    def _execute_query_retrieve(
        cls, query: Any, dtypes: dict[str, DType], engine: sa.Engine
    ) -> pd.DataFrame:
        dtypes = {name: dtype.to_pandas() for name, dtype in dtypes.items()}
        pd_version = Version(pd.__version__)

        with engine.connect() as conn:
            if pd_version >= Version("2.0"):
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
        schema = store.get_schema(stage_name)
        if store.print_materialize:
            store.logger.info(
                f"Writing table '{schema.get()}.{table.name}'",
                table_obj=table.obj,
            )
        table_name = table.name
        # TODO:
        # dtype_map = {}
        # if store.engine.dialect.name == "ibm_db_sa":
        #     # Default string target is CLOB which can't be used for indexing.
        #     # We could use VARCHAR(32000), but if we change this to VARCHAR(256)
        #     # for indexed columns, DB2 hangs.
        #     dtype_map = {
        #         col: sa.VARCHAR(256)
        #         for col in table.obj.dtypes.loc[lambda x: x == object].index
        #     }
        #     table_name = ibm_db_sa_fix_name(table_name)
        table.obj.to_pandas(use_pyarrow_extension_array=True).to_sql(
            name=table_name, con=store.engine, schema=schema.get(), index=False
        )
        store.add_indexes(table, schema)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type[polars.dataframe.DataFrame],
        namer: NameDisambiguator | None = None,
    ) -> polars.dataframe.DataFrame:
        schema = store.get_schema(stage_name).get()
        table_name = table.name
        table_name, schema = store.resolve_aliases(table_name, schema)
        connection_uri = store.engine_url.render_as_string(hide_password=False)
        df = polars.read_database(
            f'SELECT * FROM {schema}."{table_name}"', connection_uri
        )
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
        PolarsTableHook.materialize(store, table, stage_name, task_info)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type[tidypolars.Tibble],
        namer: NameDisambiguator | None = None,
    ) -> tidypolars.Tibble:
        df = PolarsTableHook.retrieve(store, table, stage_name, as_type, namer)
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
            return PandasTableHook.materialize(store, table, stage_name, task_info)
        if isinstance(t._impl, SQLTableImpl):
            table.obj = t._impl.build_select()
            return SQLAlchemyTableHook.materialize(store, table, stage_name, task_info)
        raise NotImplementedError

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type[T],
        namer: NameDisambiguator | None = None,
    ) -> T:
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        if issubclass(as_type, PandasTableImpl):
            df = PandasTableHook.retrieve(store, table, stage_name, pd.DataFrame, namer)
            return pdt.Table(PandasTableImpl(table.name, df))
        if issubclass(as_type, SQLTableImpl):
            sa_tbl = SQLAlchemyTableHook.retrieve(
                store, table, stage_name, sa.Table, namer
            )
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
    # optional dependency to ibis
    import ibis
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    ibis = None


@SQLTableStore.register_table(ibis)
class IbisTableHook(TableHook[SQLTableStore]):
    @staticmethod
    def get_con(store):
        # TODO: move this function to a better ibis specific place
        con = store.engines_other.get("ibis")
        if con is None:
            url = store.engine_url
            dialect = store.engine.dialect.name
            if dialect == "postgresql":
                con = ibis.postgres.connect(
                    host=url.host,
                    user=url.username,
                    password=url.password,
                    port=url.port,
                    database=url.database,
                )
            elif dialect == "mssql":
                con = ibis.mssql.connect(
                    host=url.host,
                    user=url.username,
                    password=url.password,
                    port=url.port,
                    database=url.database,
                )
            else:
                raise RuntimeError(
                    f"initializing ibis for {dialect} is not supported, yet "
                    "-- supported: postgresql, mssql"
                )
            store.engines_other["ibis"] = con
        return con

    @classmethod
    def can_materialize(cls, type_) -> bool:
        # Operations on a table like mutate() or join() don't change the type
        return issubclass(type_, ibis.expr.types.Table)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return issubclass(type_, ibis.expr.types.Table)

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[ibis.expr.types.Table],
        stage_name,
        task_info: TaskInfo | None,
    ):
        t = table.obj
        table = table.copy_without_obj()
        table.obj = sa.text(cls.lazy_query_str(store, t))
        return SQLAlchemyTableHook.materialize(store, table, stage_name, task_info)

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type[ibis.expr.types.Table],
        namer: NameDisambiguator | None = None,
    ) -> ibis.expr.types.Table:
        con = cls.get_con(store)
        table_name = table.name
        schema = store.get_schema(stage_name).get()
        table_name, schema = store.resolve_aliases(table_name, schema)
        for retry_iteration in range(4):
            # retry operation since it might have been terminated as a deadlock victim
            try:
                tbl = con.table(
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
    def auto_table(cls, obj: ibis.expr.types.Table):
        if obj.has_name():
            return Table(obj, obj.get_name())
        else:
            return super().auto_table(obj)

    @classmethod
    def lazy_query_str(cls, store, obj: ibis.expr.types.Table) -> str:
        return str(ibis.to_sql(obj, cls.get_con(store).name))


# endregion