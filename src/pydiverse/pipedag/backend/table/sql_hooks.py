from __future__ import annotations

import datetime
import re
import time
import warnings
from typing import Any

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.mssql import DATETIME2

from pydiverse.pipedag._typing import T
from pydiverse.pipedag.backend.table.base import TableHook
from pydiverse.pipedag.backend.table.sql import SQLTableStore
from pydiverse.pipedag.backend.table.util.sql_ddl import (
    CreateTableAsSelect,
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
    """
    Materalize pandas->sql and retrieve sql->pandas.

    We like to limit the use of types, so operations can be implemented reliably
    without the use of pandas dtype=object where every row can have different type:
      - string/varchar/clob: pd.StringDType
      - date: datetime64[ns] (we cap year in 1900..2199 and save original year
            in separate column)
      - datetime: datetime64[ns] (we cap year in 1900..2199 and save original year
            in separate column)
      - int: pd.Int64DType
      - boolean: pd.BooleanDType

    Dialect specific aspects:
      * ibm_db_sa:
        - DB2 does not support boolean type -> integer
        - We limit string columns to 256 characters since larger columns have
          trouble with adding indexes/primary keys
    """

    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pd.DataFrame)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == pd.DataFrame

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[pd.DataFrame],
        stage_name,
        task_info: TaskInfo | None,
    ):
        # we might try to avoid this copy for speedup / saving RAM
        df = table.obj.copy()
        schema = store.get_schema(stage_name)
        if store.print_materialize:
            store.logger.info(
                f"Writing table '{schema.get()}.{table.name}'",
                table_obj=table.obj,
            )
        dtype_map = {}
        table_name = table.name
        str_type = sa.String()
        if store.engine.dialect.name == "ibm_db_sa":
            # Default string target is CLOB which can't be used for indexing.
            # We could use VARCHAR(32000), but if we change this to VARCHAR(256)
            # for indexed columns, DB2 hangs.
            str_type = sa.VARCHAR(256)
            table_name = ibm_db_sa_fix_name(table_name)
        # force dtype=object to string (we require dates to be stored in datetime64[ns])
        for col in df.dtypes.loc[lambda x: x == object].index:
            if table.type_map is None or col not in table.type_map:
                df[col] = df[col].astype(str)
                dtype_map[col] = str_type
        for col in df.dtypes.loc[
            lambda x: (x != object) & x.isin([pd.Int32Dtype, pd.UInt16Dtype])
        ].index:
            dtype_map[col] = sa.INTEGER()
        for col in df.dtypes.loc[
            lambda x: (x != object)
            & x.isin([pd.Int16Dtype, pd.Int8Dtype, pd.UInt8Dtype])
        ].index:
            dtype_map[col] = sa.SMALLINT()
        if table.type_map is not None:
            dtype_map.update(table.type_map)
        # Todo: do better abstraction of dialect specific code
        if store.engine.dialect.name == "mssql":
            for col, _type in dtype_map.items():
                if _type == sa.DateTime:
                    dtype_map[col] = DATETIME2()
        df.to_sql(
            table_name,
            store.engine,
            schema=schema.get(),
            index=False,
            dtype=dtype_map,
            chunksize=100_000,
        )
        store.add_indexes(table, schema)

    @staticmethod
    def _pandas_type(sql_type):
        if isinstance(sql_type, sa.SmallInteger):
            return pd.Int16Dtype()
        elif isinstance(sql_type, sa.BigInteger):
            return pd.Int64Dtype()
        elif isinstance(sql_type, sa.Integer):
            return pd.Int32Dtype()
        elif isinstance(sql_type, sa.Boolean):
            return pd.BooleanDtype()
        elif isinstance(sql_type, sa.String):
            return pd.StringDtype()
        elif isinstance(sql_type, sa.Date):
            return "datetime64[ns]"
        elif isinstance(sql_type, sa.DateTime):
            return "datetime64[ns]"

    @staticmethod
    def _fix_cols(cols: dict[str, Any], dialect_name):
        cols = cols.copy()
        year_cols = []

        for name, col in list(cols.items()):
            if isinstance(col.type, (sa.Date, sa.DateTime)):
                if isinstance(col.type, sa.Date):
                    min_val = datetime.date(1900, 1, 1)
                    max_val = datetime.date(2199, 12, 31)
                elif isinstance(col.type, sa.DateTime):
                    min_val = datetime.datetime(1900, 1, 1, 0, 0, 0)
                    max_val = datetime.datetime(2199, 12, 31, 23, 59, 59)
                else:
                    raise

                if name + "_year" not in cols:
                    cols[name + "_year"] = sa.cast(
                        sa.func.extract("year", cols[name]), sa.Integer
                    ).label(name + "_year")
                    year_cols.append(name + "_year")
                # TODO: do better abstraction of dialect specific code
                if dialect_name == "mssql":
                    cap_min = sa.func.iif(cols[name] < min_val, min_val, cols[name])
                    cols[name] = sa.func.iif(cap_min > max_val, max_val, cap_min).label(
                        name
                    )
                else:
                    cols[name] = sa.func.least(
                        max_val, sa.func.greatest(min_val, cols[name])
                    ).label(name)
        return cols, year_cols

    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str,
        as_type: type[pd.DataFrame],
        namer: NameDisambiguator | None = None,
    ) -> pd.DataFrame:
        schema = store.get_schema(stage_name).get()
        with store.engine.connect() as conn:
            table_name = table.name
            table_name, schema = store.resolve_aliases(table_name, schema)
            for retry_iteration in range(4):
                # retry operation since it might have been terminated as a
                # deadlock victim
                try:
                    sql_table = sa.Table(
                        table_name,
                        sa.MetaData(),
                        schema=schema,
                        autoload_with=store.engine,
                    ).alias("tbl")
                    dtype_map = {c.name: cls._pandas_type(c.type) for c in sql_table.c}
                    cols, year_cols = cls._fix_cols(
                        {c.name: c for c in sql_table.c}, store.engine.dialect.name
                    )
                    dtype_map.update({col: pd.Int16Dtype() for col in year_cols})
                    query = sa.select(*cols.values()).select_from(sql_table)
                    try:
                        # works only from pandas 2.0.0 on
                        df = pd.read_sql(
                            query,
                            con=conn,
                            dtype=dtype_map,
                        )
                    except TypeError:
                        df = pd.read_sql(query, con=conn)
                        for col, _type in dtype_map.items():
                            df[col] = df[col].astype(_type)
                    break
                except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError):
                    if retry_iteration == 3:
                        raise
                    time.sleep(retry_iteration * retry_iteration * 1.3)
            return df

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        if name := obj.attrs.get("name"):
            return Table(obj, name)
        return super().auto_table(obj)


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
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        t = table.obj
        table = table.copy_without_obj()
        if isinstance(t._impl, PandasTableImpl):
            from pydiverse.transform.core.verbs import collect

            table.obj = t >> collect()
            # noinspection PyTypeChecker
            return PandasTableHook.materialize(store, table, stage_name, task_info)
        if isinstance(t._impl, SQLTableImpl):
            table.obj = t._impl.build_select()
            # noinspection PyTypeChecker
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
