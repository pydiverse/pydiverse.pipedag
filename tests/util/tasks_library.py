from __future__ import annotations

import pandas as pd
import datetime as dt

from pydiverse.pipedag import Blob, Table, materialize
import sqlalchemy as sa

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


def _get_df(data: dict[str, list], use_ext_dtype=False, cap_dates=False):
    data = data.copy()
    dtypes = {}
    for col in list(data.keys()):
        if type(data[col][0]) == dt.date:
            data[col] = [dt.datetime.combine(d, dt.time()) for d in data[col]]
        if cap_dates:
            min_datetime = dt.datetime(1900, 1, 1, 0, 0, 0)
            max_datetime = dt.datetime(2199, 12, 31, 0, 0, 0)
            min_date = dt.date(1900, 1, 1)
            max_date = dt.date(2199, 12, 31)
            if type(data[col][0]) == dt.date:
                data[col + "_year"] = [d.year for d in data[col]]
                dtypes[col + "_year"] = pd.Int16Dtype()
                data[col] = [max(min(v, max_date), min_date) for v in data[col]]
                dtypes[col] = "datetime64[ns]"
            elif type(data[col][0]) == dt.datetime:
                data[col + "_year"] = [d.year for d in data[col]]
                dtypes[col + "_year"] = pd.Int16Dtype()
                data[col] = [max(min(v, max_datetime), min_datetime) for v in data[col]]
                dtypes[col] = "datetime64[ns]"
        if use_ext_dtype:
            if type(data[col][0]) == str:
                dtypes[col] = pd.StringDtype()
            elif type(data[col][0]) == int:
                dtypes[col] = pd.Int64Dtype()
            elif type(data[col][0]) == bool:
                dtypes[col] = pd.BooleanDtype()
    return pd.DataFrame(
        {
            col: (
                pd.Series(values, dtype=dtypes[col])
                if col in dtypes
                else pd.Series(values)
            )
            for col, values in data.items()
        }
    )


@materialize(version="1.0")
def pd_dataframe(data: dict[str, list], use_ext_dtype=False, cap_dates=False):
    df = _get_df(data, use_ext_dtype, cap_dates)
    kwargs = {}
    if not cap_dates:
        kwargs["type_map"] = {
            col: sa.Date for col, items in data.items() if type(items[0]) in [dt.date]
        }
        kwargs["type_map"].update(
            {
                col: sa.DateTime
                for col, items in data.items()
                if type(items[0]) in [dt.datetime]
            }
        )
    return Table(df, **kwargs)


@materialize(input_type=pd.DataFrame, version="1.0")
def pd_dataframe_assert(df_actual: pd.DataFrame, data: dict[str, list]):
    df_expected = _get_df(data, use_ext_dtype=True, cap_dates=True)
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
