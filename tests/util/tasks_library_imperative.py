from __future__ import annotations

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import RawSql, Table, Task, materialize
from pydiverse.pipedag.core.task import TaskGetItem
from tests.util import select_as

try:
    import polars as pl
except ImportError:
    pl = None


def noop(x):
    # fail already at declare time
    assert isinstance(x, (Task, TaskGetItem))

    @materialize(input_type=pd.DataFrame, version="1.0")
    def _noop(x):
        # constant or collection of constants not supported in imperative version
        assert isinstance(x, pd.DataFrame)
        return Table(x).materialize()

    return _noop(x)


@materialize(input_type=pd.DataFrame, version="1.0")
def noop2(x):
    return Table(x).materialize()


@materialize(input_type=sa.Table, version="1.0")
def noop_sql(x):
    return Table(x).materialize()


@materialize(input_type=sa.Table, lazy=True)
def noop_lazy(x):
    return Table(x).materialize()


@materialize(input_type=pl.DataFrame if pl else 0)
def noop_polars(x):
    return Table(x).materialize()


@materialize(input_type=pl.LazyFrame if pl else 0)
def noop_lazy_polars(x):
    return Table(x).materialize()


@materialize(nout=2, version="1.0", input_type=pd.DataFrame)
def create_tuple(x, y):
    return Table(x).materialize(), Table(y).materialize()


@materialize(lazy=True)
def one_sql_lazy():
    return Table(select_as(1, "x")).materialize()


@materialize(input_type=pd.DataFrame)
def assert_table_equal(x, y, **kwargs):
    # This function explicitly has no version set to prevent it from getting cached
    pd.testing.assert_frame_equal(x, y, **kwargs)


@materialize(version="1.0", input_type=pd.DataFrame)
def take_first(table):
    return Table(table["x"][0]).materialize()


@materialize(version="1.0")
def simple_dataframe():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df).materialize()


@materialize(version="1.0")
def simple_dataframe_subtask():
    return simple_dataframe()


@materialize(version="1.0")
def simple_dataframe_debug_materialize_no_taint():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    res = Table(df, name="test_table").materialize()
    return res


@materialize(version="1.0")
def simple_dataframe_debug_materialize_twice():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    tbl = Table(df, name="simple_dataframe_dmt_%%")
    _ = tbl.materialize()
    tbl.obj.iloc[3] = [4, "4"]
    res = tbl.materialize()

    return res


def _get_df_query():
    try:
        unions = [
            sa.select([sa.literal(i).label("col1"), sa.literal(str(i)).label("col2")])
            for i in range(4)
        ]
    except sa.exc.ArgumentError:
        # this works from sqlalchemy 2.0.0 on
        unions = [
            sa.select(sa.literal(i).label("col1"), sa.literal(str(i)).label("col2"))
            for i in range(4)
        ]
    return unions[0].union_all(*unions[1:])


@materialize(lazy=True)
def simple_lazy_table():
    query = _get_df_query()
    return Table(query).materialize()


@materialize(lazy=True)
def simple_lazy_table_with_pk():
    query = _get_df_query()
    return Table(query, primary_key="col1").materialize()


@materialize(version="1.0")
def pd_dataframe(data: dict[str, list]):
    return Table(pd.DataFrame(data)).materialize()


@materialize(lazy=True)
def as_raw_sql(sql: str):
    return RawSql(sql).materialize()


@materialize()
def exception(x, r: bool):
    if r:
        raise Exception("THIS EXCEPTION IS EXPECTED")
    return x
