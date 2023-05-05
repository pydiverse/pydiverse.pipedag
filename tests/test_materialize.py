from __future__ import annotations

import pandas as pd
import sqlalchemy as sa
import datetime as dt
import pytest
import structlog

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.backend.table.sql import sa_select
from pydiverse.pipedag.context import RunContext, StageLockContext
from pydiverse.pipedag.util.config import PipedagConfig

from tests.util import select_as, tasks_library as m

# parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instance import *


def test_materialize_literals():
    """
    Check that all json serializable literals get stored and retrieved
    correctly from the table / metadata store.
    """

    def materialize_and_retrieve(value):
        v = m.noop(value)
        return m.assert_equal(value, v)

    with Flow("flow") as f:
        with Stage("stage"):
            materialize_and_retrieve(None)

            materialize_and_retrieve(0)
            materialize_and_retrieve(1)

            # max 32bit
            materialize_and_retrieve(-2147483648)
            materialize_and_retrieve(2147483647)

            # max 64bit
            materialize_and_retrieve(-9223372036854775808)
            materialize_and_retrieve(9223372036854775807)

            # in python json we even have infinite precision integers:
            materialize_and_retrieve(-9223372036854775809)
            materialize_and_retrieve(9223372036854775808)

            materialize_and_retrieve(3.14)
            materialize_and_retrieve(-2.71)

            materialize_and_retrieve("")
            materialize_and_retrieve("a string")
            materialize_and_retrieve("a" * 256)

            materialize_and_retrieve([0, 1, 2, 3])
            materialize_and_retrieve((0, 1, 2, 3))

            materialize_and_retrieve({"0": 0, "1": 1})

            materialize_and_retrieve(
                {
                    "key": [0, 1, 2, 3],
                    "numbers": {
                        "zero": 0,
                        "one": 1,
                    },
                    "nested_lists": [[[None]]],
                }
            )

    assert f.run().successful


def test_materialize_table():
    with Flow("flow") as f:
        with Stage("stage"):
            x = m.simple_dataframe()

            m.assert_table_equal(x, x)

    assert f.run().successful


def test_materialize_table_values():
    values_dict_of_lists = {
        "x": [0, 1],
        "x32": [-2147483648, 2147483647],
        "x64": [-9223372036854775808, 9223372036854775807],
        "str256": ["", "a" * 256],
        "b": [True, False],
        "d": [dt.date(1900, 1, 1), dt.date(2199, 12, 31)],
        "dt": [dt.datetime(1900, 1, 1, 0, 0, 0), dt.datetime(2199, 12, 31, 23, 59, 59)],
        "d2": [dt.date(1, 1, 1), dt.date(9999, 12, 31)],
        "dt2": [dt.datetime(1, 1, 1, 0, 0, 0), dt.datetime(9999, 12, 31, 23, 59, 59)],
    }

    with Flow("flow") as f:
        with Stage("stage_0"):
            t_0 = m.pd_dataframe(values_dict_of_lists, cap_dates=True)
            t_1 = m.pd_dataframe(values_dict_of_lists, cap_dates=False)
            t_2 = m.noop(t_0)
            t_3 = m.noop(t_1)
            t_4 = m.noop_lazy(t_0)
            t_5 = m.noop_lazy(t_1)
            for t in [t_0, t_1, t_2, t_3, t_4, t_5]:
                m.pd_dataframe_assert(t, values_dict_of_lists)

    assert f.run().successful


def test_materialize_table_twice():
    with Flow("flow") as f:
        with Stage("stage"):
            x = m.simple_dataframe()
            y = m.simple_dataframe()

            m.assert_table_equal(x, y)

    assert f.run().successful


def test_materialize_blob():
    with Flow("flow") as f:
        with Stage("stage_0"):
            x = m.object_blob({"a": 12, "b": 24})
            y = m.as_blob(x)
            m.assert_blob_equal(x, y)

        with Stage("stage_1"):
            one = m.one()
            one_blob = m.as_blob(m.one())
            m.assert_equal(one, one_blob)

    assert f.run().successful


def test_failure():
    with Flow("flow") as f:
        with Stage("failure_stage"):
            m.exception(0, True)

    with pytest.raises(Exception, match="THIS EXCEPTION IS EXPECTED"):
        f.run(fail_fast=True)

    with Flow("flow") as f:
        with Stage("failure_stage"):
            x = m.exception(0, True)
            m.noop(x)

    with pytest.raises(Exception, match="THIS EXCEPTION IS EXPECTED"):
        f.run(fail_fast=True)


def test_return_pipedag_config():
    with Flow("flow") as f:
        with Stage("failure_stage"):
            m.noop(PipedagConfig.default)

    with pytest.raises(TypeError, match="PipedagConfig"):
        f.run(fail_fast=True)


def test_materialize_memo_literal():
    # A flow should be able to contain the same task with the same inputs
    # more than once and still run successfully.

    with Flow("flow") as f:
        with Stage("stage_0"):
            t_0 = m.noop(1)
            t_1 = m.noop(1)

            one = m.one()
            t_2 = m.noop(one)
            t_3 = m.noop(one)

        with Stage("stage_1"):
            t_4 = m.noop(1)
            t_5 = m.noop(t_4)
            t_6 = m.noop(t_5)
            t_7 = m.noop(t_0)

        with Stage("stage_2"):
            m.assert_equal(t_0, t_4)
            m.assert_equal(t_1, t_5)
            m.assert_equal(t_2, t_6)
            m.assert_equal(t_3, t_7)

    assert f.run().successful


def test_materialize_memo_table():
    with Flow("flow") as f:
        with Stage("stage_0"):
            t_0 = m.pd_dataframe({"x": [0, 0], "y": [0, 0]})
            t_1 = m.pd_dataframe({"x": [0, 0], "y": [0, 0]})
            t_2 = m.pd_dataframe({"x": [0, 0], "y": [0, 0]})
            t_3 = m.pd_dataframe({"x": [0, 0], "y": [0, 0]})

            m.assert_table_equal(t_0, t_1)
            m.assert_table_equal(t_1, t_2)
            m.assert_table_equal(t_2, t_3)

        with Stage("stage_1"):
            t_4 = m.pd_dataframe({"x": [0, 0], "y": [0, 0]})
            t_5 = m.noop(t_4)
            t_6 = m.noop(t_5)
            t_7 = m.noop(t_0)
            t_8 = m.noop_sql(t_4)
            t_9 = m.noop_sql(t_5)
            t_10 = m.noop_sql(t_0)
            t_11 = m.noop_lazy(t_4)
            t_12 = m.noop_lazy(t_5)
            t_13 = m.noop_lazy(t_0)

        with Stage("stage_2"):
            m.assert_table_equal(t_0, t_4)
            m.assert_table_equal(t_1, t_5)
            m.assert_table_equal(t_2, t_6)
            m.assert_table_equal(t_3, t_7)
            m.assert_table_equal(t_1, t_8)
            m.assert_table_equal(t_2, t_9)
            m.assert_table_equal(t_3, t_10)
            m.assert_table_equal(t_1, t_11)
            m.assert_table_equal(t_2, t_12)
            m.assert_table_equal(t_3, t_13)

    assert f.run().successful


def test_materialize_memo_with_failure():
    with Flow("flow") as f:
        with Stage("stage"):
            t_0 = m.exception(1, False)
            t_1 = m.exception(1, False)

            one = m.one()
            t_2 = m.exception(1, True)
            t_3 = m.exception(one, True)
            t_4 = m.exception(t_3, False)

    assert not f.run(fail_fast=False).successful


def test_stage_ref_counter():
    def check_rc(stage, expected):
        @materialize(lazy=True, input_type=pd.DataFrame)
        def _m_check_rc(x):
            assert RunContext.get().get_stage_ref_count(stage) == expected
            return x

        return _m_check_rc

    # Super simple case with just two task inside one stage
    with Flow("flow") as f:
        with Stage("stage") as s:
            task_1 = m.one()
            task_2 = m.two()

            # One reference from the check_rc and commit
            check_rc(s, 2)([task_1, task_2])

    assert f.run().successful

    # Multiple tasks with interdependency inside one stage
    with Flow("flow") as f:
        with Stage("stage") as s:
            task_1 = m.one()
            task_2 = m.two()
            task_tuple = m.noop((task_1, task_2))
            # One reference from assert, noop, assert and commit
            task_tuple = check_rc(s, 4)(task_tuple)
            task_tuple = m.noop(task_tuple)
            # One reference from assert and commit
            check_rc(s, 2)(task_tuple)

    assert f.run().successful

    # Multiple tasks spread over multiple stages
    with Flow("flow") as f:
        with Stage("stage_1") as s1:
            task_1 = m.one()
            # One reference from assert, noop, assert, commit and downstream
            task_1 = check_rc(s1, 5)(task_1)
            task_1 = m.noop(task_1)
            # One reference from assert, commit and downstream
            check_rc(s1, 3)(task_1)

        with Stage("stage_2") as s2:
            task_2 = m.two()
            task_2 = m.noop([task_2])
            check_rc(s2, 3)(task_2)

        with Stage("stage_3") as s3:
            task_tuple = m.noop((task_1, task_2))
            # Check that s1 and s2 have been freed
            x = check_rc(s1, 0)(task_tuple)
            x = check_rc(s2, 0)(x)
            check_rc(s3, 2)(x)

    assert f.run().successful


def test_nout_and_getitem():
    with Flow("flow") as f:
        with Stage("stage_0"):
            x = m.noop((0, 1))
            zero, one = m.create_tuple(x[0], x[1])

            m.assert_equal(zero, 0)
            m.assert_equal(one, m.one())

        with Stage("stage_1"):
            d = m.noop({"a": [0, 1, 2, 3], "b": [3, 2, 1, 0]})

            a = d["a"]
            b = d["b"]

            for i in range(4):
                m.assert_equal(a[i], i)
                m.assert_equal(a[i], b[3 - i])

    assert f.run().successful


def test_name_mangling_tables():
    @materialize
    def table_task_1():
        return Table(pd.DataFrame({"x": [1]}), name="table_%%")

    @materialize
    def table_task_2():
        return Table(pd.DataFrame({"x": [2]}), name="table_%%")

    with Flow() as f:
        with Stage("stage_1"):
            table_1 = table_task_1()
            table_2 = table_task_2()

    with StageLockContext():
        result = f.run()
        assert result.get(table_1, as_type=pd.DataFrame)["x"][0] == 1
        assert result.get(table_2, as_type=pd.DataFrame)["x"][0] == 2


def test_name_mangling_lazy_tables():
    @materialize(lazy=True)
    def lazy_task_1():
        return Table(select_as(1, "x"), name="table_%%")

    @materialize(lazy=True)
    def lazy_task_2():
        return Table(select_as(2, "x"), name="table_%%")

    with Flow() as f:
        with Stage("stage_1"):
            lazy_1 = lazy_task_1()
            lazy_2 = lazy_task_2()

    with StageLockContext():
        result = f.run()
        assert result.get(lazy_1, as_type=pd.DataFrame)["x"][0] == 1
        assert result.get(lazy_2, as_type=pd.DataFrame)["x"][0] == 2


def test_name_mangling_lazy_table_cache_fn():
    # Only the cache fn output of these two tasks is different
    @materialize(lazy=True, cache=lambda: 1, name="lazy_task")
    def lazy_task_1():
        return Table(select_as(1, "x"))

    @materialize(lazy=True, cache=lambda: 2, name="lazy_task")
    def lazy_task_2():
        return Table(select_as(2, "x"))

    with Flow() as f:
        with Stage("stage_1"):
            lazy_1 = lazy_task_1()
            lazy_2 = lazy_task_2()

    with StageLockContext():
        result = f.run()
        assert result.get(lazy_1, as_type=pd.DataFrame)["x"][0] == 1
        assert result.get(lazy_2, as_type=pd.DataFrame)["x"][0] == 2


@materialize(lazy=True)
def _lazy_task_1():
    return Table(select_as(1, "x"), name="t1", primary_key="x")


@materialize(lazy=True, nout=2)
def _lazy_task_2():
    return Table(select_as(1, "x"), name="t21", indexes=[["x"]]), Table(
        select_as(2, "x"), name="t22", primary_key=["x"]
    )


@materialize(lazy=True, input_type=sa.Table)
def _lazy_join(src1: sa.Table, src2: sa.Table):
    query = sa_select([src1.c.x, src2.c.x.label("x2")]).select_from(
        src1.outerjoin(src2, src1.c.x == src2.c.x)
    )
    return Table(query, "t3_%%", indexes=[["x2"], ["x", "x2"]])


@materialize(version="1.0")
def _sql_task_1():
    return Table(select_as(1, "x"), name="t1", primary_key="x")


@materialize(version="1.0", nout=2)
def _sql_task_2():
    return Table(select_as(1, "x"), name="t21", indexes=[["x"]]), Table(
        select_as(2, "x"), name="t22", primary_key=["x"]
    )


@materialize(version="1.0", input_type=sa.Table)
def _sql_join(src1: sa.Table, src2: sa.Table):
    query = sa_select([src1.c.x, src2.c.x.label("x2")]).select_from(
        src1.outerjoin(src2, src1.c.x == src2.c.x)
    )
    return Table(query, "t3_%%", indexes=[["x2"], ["x", "x2"]])


@materialize(version="1.0")
def _eager_task_1():
    df = pd.DataFrame(dict(x=[1]))
    return Table(df, name="t1", primary_key="x")


@materialize(version="1.0", nout=2)
def _eager_task_2():
    df1 = pd.DataFrame(dict(x=[1]))
    df2 = pd.DataFrame(dict(x=[2]))
    return Table(df1, name="t21", indexes=[["x"]]), Table(
        df2, name="t22", primary_key=["x"]
    )


@materialize(version="1.0", input_type=pd.DataFrame)
def _eager_join(src1: pd.DataFrame, src2: pd.DataFrame):
    src2["x2"] = src2["x"]
    join = src1.merge(src2, on="x", how="left")
    return Table(join, "t3_%%", indexes=[["x2"], ["x", "x2"]])


@pytest.mark.parametrize(
    "task_1,task_2,noop,join",
    [
        # communicate with same kind
        (_lazy_task_1, _lazy_task_2, m.noop_lazy, _lazy_join),
        (_sql_task_1, _sql_task_2, m.noop_sql, _sql_join),
        (_eager_task_1, _eager_task_2, m.noop, _eager_join),
        # communicate to sql
        (_lazy_task_1, _lazy_task_2, m.noop_sql, _sql_join),
        (_eager_task_1, _eager_task_2, m.noop_sql, _sql_join),
        # communicate to lazy
        (_sql_task_1, _sql_task_2, m.noop_lazy, _lazy_join),
        (_eager_task_1, _eager_task_2, m.noop_lazy, _lazy_join),
        # communicate to eager
        (_lazy_task_1, _lazy_task_2, m.noop, _eager_join),
        (_sql_task_1, _sql_task_2, m.noop, _eager_join),
    ],
)
def test_task_and_stage_communication(task_1, task_2, noop, join):
    logger = structlog.getLogger("test_task_and_stage_communication")
    with Flow() as f:
        with Stage("stage_1"):
            t1 = task_1()
            t21, t22 = task_2()
            same_stage = noop(t22)
            same_stage2 = join(t1, t21)
        with Stage("stage_2"):
            next_stage = noop(t22)
            next_stage2 = join(t1, t21)
            next_same_stage = join(t1, next_stage)

    for i in range(3):
        with StageLockContext():
            logger.info("## Running flow", iteration=i)
            result = f.run()
            logger.info("#    checking flow", iteration=i)
            assert result.successful
            assert result.get(t1, as_type=pd.DataFrame)["x"][0] == 1
            assert result.get(t21, as_type=pd.DataFrame)["x"][0] == 1
            assert result.get(t22, as_type=pd.DataFrame)["x"][0] == 2
            assert result.get(same_stage, as_type=pd.DataFrame)["x"][0] == 2
            assert result.get(same_stage2, as_type=pd.DataFrame)["x"][0] == 1
            assert result.get(same_stage2, as_type=pd.DataFrame)["x2"][0] == 1
            assert result.get(next_stage, as_type=pd.DataFrame)["x"][0] == 2
            assert result.get(next_stage2, as_type=pd.DataFrame)["x"][0] == 1
            assert result.get(next_stage2, as_type=pd.DataFrame)["x2"][0] == 1
            assert result.get(next_same_stage, as_type=pd.DataFrame)["x"][0] == 1
            assert result.get(next_same_stage, as_type=pd.DataFrame)["x2"].isna().all()
