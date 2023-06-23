from __future__ import annotations

import pandas as pd
import datetime as dt

from pydiverse.pipedag import Blob, Table, materialize
import sqlalchemy as sa
import sqlalchemy.dialects

from pydiverse.pipedag.backend.table.util.pandas import adjust_pandas_types
from pydiverse.pipedag.context import ConfigContext


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


@materialize(nout=2, version="1.0")
def create_tuple(x, y):
    return x, y


@materialize(version="1.0")
def one():
    return 1


@materialize(version="1.0")
def two():
    return 2


@materialize()
def assert_equal(x, y):
    # This function explicitly has no version set to prevent it from getting cached
    assert x == y, f"{x} != {y}"


@materialize(input_type=pd.DataFrame)
def assert_table_equal(x, y, **kwargs):
    # This function explicitly has no version set to prevent it from getting cached
    pd.testing.assert_frame_equal(x, y, **kwargs)


@materialize()
def assert_blob_equal(x, y):
    # This function explicitly has no version set to prevent it from getting cached
    assert x == y


@materialize(version="1.0")
def simple_dataframe():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df)


@materialize(version="1.0")
def simple_dataframe_with_pk():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df, primary_key="col1")


@materialize(version="1.0")
def simple_dataframe_with_pk2():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df, primary_key=["col1", "col2"])


@materialize(version="1.0")
def simple_dataframe_with_index():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df, primary_key=["col1"], indexes=[["col2"]])


@materialize(version="1.0")
def simple_dataframe_with_indexes():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df, primary_key=["col1"], indexes=[["col2"], ["col2", "col1"]])


@materialize(nout=2, version="1.0")
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


def _get_df(data: dict[str, list], cap_dates=False):
    """Constructs a pandas dataframe from a dictionary"""
    df_data = data.copy()
    df_dtypes = {}

    min_datetime = dt.datetime(1700, 1, 1, 0, 0, 0)
    max_datetime = dt.datetime(2200, 1, 1, 0, 0, 0)
    min_date = dt.date(1700, 1, 1)
    max_date = dt.date(2200, 1, 1)

    for col in data:
        if cap_dates:
            if type(data[col][0]) == dt.date:
                df_data[col + "_year"] = [getattr(d, "year", None) for d in data[col]]
                df_dtypes[col + "_year"] = pd.Int16Dtype()
                df_data[col] = [
                    max(min(v, max_date), min_date) if v is not None else None
                    for v in data[col]
                ]
                df_dtypes[col] = "datetime64[ns]"
            elif type(data[col][0]) == dt.datetime:
                df_data[col + "_year"] = [getattr(d, "year", None) for d in data[col]]
                df_dtypes[col + "_year"] = pd.Int16Dtype()
                df_data[col] = [
                    max(min(v, max_datetime), min_datetime) if v is not None else None
                    for v in data[col]
                ]
                df_dtypes[col] = "datetime64[ns]"

        if type(data[col][0]) == bool:
            df_dtypes[col] = pd.BooleanDtype()

    df = pd.DataFrame(
        {
            name: pd.Series(val, dtype=df_dtypes.get(name))
            for name, val in df_data.items()
        }
    )

    return df


@materialize(version="1.0")
def pd_dataframe(data: dict[str, list], cap_dates=False):
    df = _get_df(data, cap_dates)
    type_map = {}
    if not cap_dates:
        type_map.update(
            {
                col: sa.Date()
                for col, items in data.items()
                if isinstance(items[0], dt.date)
            }
        )
        type_map.update(
            {
                col: sa.DateTime()
                for col, items in data.items()
                if isinstance(items[0], dt.datetime)
            }
        )

    if ConfigContext.get().store.table_store.engine.dialect.name == "mssql":
        for col, type_ in type_map.items():
            if isinstance(type_, sa.DateTime):
                type_map[col] = sa.dialects.mssql.DATETIME2()

    return Table(df, type_map=type_map)


@materialize(input_type=pd.DataFrame)
def pd_dataframe_assert(df_actual: pd.DataFrame, data: dict[str, list]):
    df_expected = _get_df(data, cap_dates=True)
    df_expected = adjust_pandas_types(df_expected)
    if ConfigContext.get().store.table_store.engine.dialect.name == "ibm_db_sa":
        for col in data:
            if type(data[col][0]) == bool:
                df_expected[col] = df_expected[col].astype(pd.Int16Dtype())
    pd.testing.assert_frame_equal(df_expected, df_actual)


@materialize(version="1.0")
def as_blob(x):
    return Blob(x)


class _SomeClass:
    def __eq__(self, other):
        return self.__dict__ == other.__dict__


@materialize(version="1.0")
def object_blob(x: dict):
    instance = _SomeClass()
    instance.__dict__.update(x)

    return Blob(instance)


@materialize()
def exception(x, r: bool):
    if r:
        raise Exception("THIS EXCEPTION IS EXPECTED")
    return x
