from __future__ import annotations

import pandas as pd

from pydiverse.pipedag import Blob, Table, materialize
import sqlalchemy as sa


@materialize(input_type=pd.DataFrame, version="1.0")
def noop(x):
    return x


@materialize(input_type=pd.DataFrame, version="1.0")
def noop2(x):
    return x


@materialize(input_type=sa.Table, version="1.0")
def noop_sql(x):
    return x


@materialize(input_type=sa.Table, lazy=True)
def noop_lazy(x):
    return x


@materialize(nout=2)
def create_tuple(x, y):
    return x, y


@materialize
def one():
    return 1


@materialize
def two():
    return 2


@materialize
def assert_equal(x, y):
    assert x == y, f"{x} != {y}"
    return x, y


@materialize(input_type=pd.DataFrame)
def assert_table_equal(x, y):
    pd.testing.assert_frame_equal(x, y)


@materialize
def assert_blob_equal(x, y):
    assert x == y


@materialize
def simple_dataframe():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df)


@materialize
def simple_dataframe_with_pk():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df, primary_key="col1")


@materialize
def simple_dataframe_with_pk2():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df, primary_key=["col1", "col2"])


@materialize
def simple_dataframe_with_index():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df, primary_key=["col1"], indexes=[["col2"]])


@materialize
def simple_dataframe_with_indexes():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df, primary_key=["col1"], indexes=[["col2"], ["col2", "col1"]])


@materialize(nout=2)
def simple_dataframes_with_indexes_task():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(
        df, "dfA", primary_key=["col1"], indexes=[["col2"], ["col2", "col1"]]
    ), Table(df, "dfB", primary_key=["col1"], indexes=[["col2"], ["col2", "col1"]])


def simple_dataframes_with_indexes():
    res, _ = simple_dataframes_with_indexes_task()
    return res


def _get_df_query():
    unions = [
        sa.select([sa.literal(i).label("col1"), sa.literal(str(i)).label("col2")])
        for i in range(4)
    ]
    return unions[0].union_all(*unions[1:])


@materialize(lazy=True)
def simple_lazy_table():
    query = _get_df_query()
    return Table(query)


@materialize(lazy=True)
def simple_lazy_table_with_pk():
    query = _get_df_query()
    return Table(query, primary_key="col1")


@materialize(lazy=True)
def simple_lazy_table_with_pk2():
    query = _get_df_query()
    return Table(query, primary_key=["col1", "col2"])


@materialize(lazy=True)
def simple_lazy_table_with_index():
    query = _get_df_query()
    return Table(query, primary_key=["col1"], indexes=[["col2"]])


@materialize(lazy=True)
def simple_lazy_table_with_indexes():
    query = _get_df_query()
    return Table(query, indexes=[["col2"], ["col2", "col1"]])


@materialize
def pd_dataframe(data: dict[str, list]):
    df = pd.DataFrame(data)
    return Table(df)


@materialize
def as_blob(x):
    return Blob(x)


class _SomeClass:
    def __eq__(self, other):
        return self.__dict__ == other.__dict__


@materialize
def object_blob(x: dict):
    instance = _SomeClass()
    instance.__dict__.update(x)

    return Blob(instance)


@materialize
def exception(x, r: bool):
    if r:
        raise Exception("THIS EXCEPTION IS EXPECTED")
    return x
