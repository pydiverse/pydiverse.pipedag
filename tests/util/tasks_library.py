from __future__ import annotations

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import inspect

from pydiverse.pipedag import Blob, RawSql, Table, materialize
from pydiverse.pipedag.backend.table.sql.ddl import MAX_LENGTH_PK
from pydiverse.pipedag.context import TaskContext
from pydiverse.pipedag.debug import materialize_table
from tests.util import select_as

try:
    import polars as pl
except ImportError:
    pl = None


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


@materialize(input_type=pl.DataFrame if pl else "<polars task without polars>")
def noop_polars(x):
    return Table(x)


@materialize(input_type=pl.LazyFrame if pl else "<polars task without polars>")
def noop_lazy_polars(x):
    return Table(x)


@materialize(input_type=pd.DataFrame, version="1.0")
def noop_subtask(x):
    @materialize(input_type=pd.DataFrame, version="1.0")
    def _noop(x):
        return Table(x)

    # This is somewhat crazy behavior to call a task within a task. However, since we
    # support calling tasks outside of `with Flow`, this will work for dataframe tasks.
    return _noop(x)


@materialize(input_type=sa.Table, lazy=True)
def noop_subtask_lazy(x):
    @materialize(input_type=sa.Table, lazy=True)
    def _noop(x):
        return Table(x)

    # This is somewhat crazy behavior to call a task within a task. However, since we
    # support calling tasks outside of `with Flow`, this will work for same type.
    return _noop(x)


@materialize(input_type=pd.DataFrame, version="1.0")
def noop_subtask_fail_input_type(x):
    @materialize(input_type=sa.Table, version="1.0")
    def _noop(x):
        return Table(x)

    # calling task with different input type should fail with good error message
    # because we call subtasks without separate dematerialization
    return _noop(x)


@materialize(nout=2, version="1.0", input_type=pd.DataFrame)
def create_tuple(x, y):
    return x, y


@materialize(version="1.0")
def one():
    return 1


@materialize(lazy=True)
def one_sql_lazy():
    return select_as(1, "x")


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


@materialize(version="1.0", input_type=pd.DataFrame)
def take_first(table, as_int=False):
    ret = table["x"][0]
    if as_int:
        # might be needed to avoid returning unserializable numpy.int64
        # we could also consider JSON serializing numpy equivalents of primitive types
        # either conserving them being numpy types or converting them to primitive types
        return int(ret)
    return ret


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
def simple_dataframe_subtask():
    # This is somewhat crazy behavior to call a task within a task. However, since we
    # support calling tasks outside of `with Flow`, this will work for dataframe tasks.
    return simple_dataframe()


@materialize(version="1.0", nout=2)
def simple_dataframe_uncompressed():
    return (
        Table(
            pd.DataFrame(dict(COMPRESSION=["N"], ROWCOMPMODE=[" "])),
            "simple_df_properties",
        ),
        simple_dataframe(),
    )


@materialize(nout=2)
def simple_dataframe_compressed_one_method():
    return Table(
        pd.DataFrame(dict(COMPRESSION=["R"], ROWCOMPMODE=["S"])),
        "df_compressed_1_properties",
    ), _simple_dataframe_materialization_details(
        "df_compressed_1", "static_compression"
    )


@materialize(nout=2)
def simple_dataframe_compressed_two_methods():
    return Table(
        pd.DataFrame(dict(COMPRESSION=["B"], ROWCOMPMODE=["A"])),
        "df_compressed_2_properties",
    ), _simple_dataframe_materialization_details(
        "df_compressed_2", "adaptive_value_compression"
    )


def _simple_dataframe_materialization_details(name=None, materialization_details=None):
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df, name=name, materialization_details=materialization_details)


@materialize(version="1.0")
def simple_dataframe_debug_materialize_no_taint():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    res = Table(df, name="test_table")
    materialize_table(res, flag_task_debug_tainted=False, debug_suffix="debug")
    return res


@materialize(version="1.0")
def simple_dataframe_debug_materialize_twice():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    res = Table(df)
    materialize_table(res, flag_task_debug_tainted=True, debug_suffix="debug")

    df.iloc[3] = [4, "4"]
    res = Table(df)
    materialize_table(res, flag_task_debug_tainted=True, debug_suffix="debug")

    return res


def complex_imperative_materialize(in_: Table):
    @materialize(lazy=True, input_type=sa.Table)
    def _temp_res1(in_: sa.Table):
        query = sa.select("*").select_from(in_)
        return Table(query)

    @materialize(lazy=True, input_type=sa.Table)
    def _temp_res2(in_: sa.Table):
        query1 = sa.select("*").select_from(in_)
        t1 = Table(query1).materialize()

        query2 = sa.select("*").select_from(t1)
        t2 = Table(query2).materialize()

        query3 = sa.select("*").select_from(t2)
        return Table(query3)

    @materialize(lazy=True, input_type=sa.Table)
    def _temp_res3(_temp_res1: sa.Table, _temp_res2: sa.Table):
        _ = _temp_res2
        query = sa.select("*").select_from(_temp_res1)
        return Table(query)

    temp_res1 = _temp_res1(in_)
    temp_res2 = _temp_res2(temp_res1)
    return _temp_res3(temp_res1, temp_res2)


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


@materialize(nout=2, lazy=False)
def simple_table_compressed_one_method():
    query = _get_df_query()
    return Table(
        pd.DataFrame(dict(COMPRESSION=["V"], ROWCOMPMODE=[" "])),
        "compress_1_properties",
    ), Table(query, name="compress_one", materialization_details="value_compression")


@materialize(version="1.0")
def compression_properties_adaptive_value_compression_db2():
    return Table(
        pd.DataFrame(dict(COMPRESSION=["B"], ROWCOMPMODE=["A"])),
        "compress_2_properties",
    )


@materialize(nout=2, lazy=False)
def simple_table_compressed_two_methods():
    query = _get_df_query()
    return compression_properties_adaptive_value_compression_db2(), Table(
        query, name="compress_two", materialization_details="adaptive_value_compression"
    )


@materialize(nout=2, lazy=False)
def simple_table_default_compressed():
    # The stage in test_compression has
    # materialization_details="adaptive_value_compression".
    # This justifies the use of compression_properties_adaptive_value_compression_db2().
    query = _get_df_query()
    return compression_properties_adaptive_value_compression_db2(), Table(
        query, name="compress_two"
    )


@materialize(version="1.0")
def pd_dataframe(data: dict[str, list]):
    return pd.DataFrame(data)


@materialize(version="1.0")
def as_blob(x):
    return Blob(x)


@materialize(lazy=True)
def as_raw_sql(sql: str):
    return RawSql(sql)


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


def get_task_logger():
    return TaskContext.get().task.logger


@materialize(input_type=sa.Table)
def check_pk_length(x: sa.Table):
    pks = inspect(x).primary_key
    if isinstance(pks, list):
        pk_name = pks[0].name
        assert len(pk_name) <= MAX_LENGTH_PK
