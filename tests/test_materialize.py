from __future__ import annotations

from concurrent.futures.process import BrokenProcessPool

import pandas as pd
import pytest
import sqlalchemy as sa
import structlog

from pydiverse.pipedag import ConfigContext, Flow, Stage, Table, materialize
from pydiverse.pipedag.context import FinalTaskState, RunContext, StageLockContext
from pydiverse.pipedag.context.context import CacheValidationMode
from pydiverse.pipedag.context.trace_hook import PrintTraceHook
from pydiverse.pipedag.core.config import PipedagConfig

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import (
    ALL_INSTANCES,
    ORCHESTRATION_INSTANCES,
    skip_instances,
    with_instances,
)
from tests.util import select_as, swallowing_raises
from tests.util import tasks_library as m
from tests.util import tasks_library_imperative as m2

pytestmark = [with_instances(ALL_INSTANCES, ORCHESTRATION_INSTANCES)]


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


@pytest.mark.parametrize("imperative", [False, True])
def test_materialize_table(imperative):
    _m = m if not imperative else m2
    with Flow("flow") as f:
        with Stage("stage"):
            x = _m.simple_dataframe()
            _m.assert_table_equal(x, x)

    assert f.run().successful


@pytest.mark.parametrize("imperative", [False, True])
def test_materialize_table_subtask(imperative):
    _m = m if not imperative else m2
    with Flow("flow") as f:
        with Stage("stage"):
            x = _m.simple_dataframe_subtask()
            y = _m.noop_subtask(x)
            z = _m.noop_subtask_lazy(x)
            _m.assert_table_equal(x, y)
            _m.assert_table_equal(x, z)

    assert f.run().successful


@pytest.mark.parametrize("imperative", [False, True])
def test_materialize_table_subtask_fail_input_type(imperative):
    _m = m if not imperative else m2
    with Flow("flow") as f:
        with Stage("stage"):
            x = _m.simple_dataframe_subtask()
            _ = _m.noop_subtask_fail_input_type(x)
    with pytest.raises(RuntimeError, match="does not match parent task input type"):
        try:
            f.run()
        except BrokenProcessPool as e:
            # TODO: find out whether we can avoid BrokenProcessPool from happening
            raise RuntimeError(
                "does not match parent task input type + BrokenProcessPool"
            ) from e


@pytest.mark.parametrize("imperative", [False, True])
def test_materialize_table_twice(imperative):
    _m = m if not imperative else m2
    with Flow("flow") as f:
        with Stage("stage"):
            x = _m.simple_dataframe()
            y = _m.simple_dataframe()

            m.assert_table_equal(x, y)

    assert f.run().successful


@pytest.mark.parametrize("imperative", [False, True])
def test_debug_materialize_table_no_taint(imperative):
    _m = m if not imperative else m2
    with Flow("flow") as f:
        with Stage("stage"):
            x = _m.simple_dataframe_debug_materialize_no_taint()
            _m.assert_table_equal(x, x)

    assert f.run().successful


@pytest.mark.parametrize("imperative", [False, True])
def test_debug_materialize_table_twice(imperative):
    _m = m if not imperative else m2
    with Flow("flow") as f:
        with Stage("stage"):
            x = _m.simple_dataframe_debug_materialize_twice()
            _m.assert_table_equal(x, x)

    with pytest.raises(RuntimeError, match="interactive debugging"):
        assert f.run().successful


@with_instances("postgres")
def test_imperative_minimal_example():
    with Flow("flow") as f:
        with Stage("input"):
            in_ = m.simple_dataframe()
        with Stage("stage"):
            _ = m.complex_imperative_materialize(in_)

    trace_hook = PrintTraceHook()  # can be used to debug caching issues
    result1 = f.run(
        cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID,
        trace_hook=trace_hook,
    )

    result2 = f.run(trace_hook=trace_hook)

    print("Run 1:")
    for task in f.tasks:
        print(f"{task.name}: {result1.task_states.get(task)}")

    print("Run 2:")
    for task in f.tasks:
        print(f"{task.name}: {result2.task_states.get(task)}")

    # Expectation: In Run 2 all tasks should be cached
    for task in f.tasks:
        if not task.name.startswith("Commit"):
            assert result2.task_states.get(task) == FinalTaskState.CACHE_VALID


@with_instances("postgres")
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

    with swallowing_raises(Exception, match="THIS EXCEPTION IS EXPECTED"):
        f.run(fail_fast=True)

    with Flow("flow") as f:
        with Stage("failure_stage"):
            x = m.exception(0, True)
            m.noop(x)

    with swallowing_raises(Exception, match="THIS EXCEPTION IS EXPECTED"):
        f.run(fail_fast=True)


def test_return_pipedag_config():
    with Flow("flow") as f:
        with Stage("failure_stage"):
            m.noop(PipedagConfig.default)

    with swallowing_raises(TypeError, match="PipedagConfig"):
        f.run(fail_fast=True)


def test_return_config_context():
    with Flow("flow") as f:
        with Stage("failure_stage"):
            m.noop(PipedagConfig.default.get())

    with swallowing_raises(TypeError, match="ConfigContext"):
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


@pytest.mark.parametrize("imperative", [False, True])
def test_materialize_memo_table(imperative):
    _m = m if not imperative else m2
    with Flow("flow") as f:
        with Stage("stage_0"):
            t_0 = _m.pd_dataframe({"x": [0, 0], "y": [0, 0]})
            t_1 = _m.pd_dataframe({"x": [0, 0], "y": [0, 0]})
            t_2 = _m.pd_dataframe({"x": [0, 0], "y": [0, 0]})
            t_3 = _m.pd_dataframe({"x": [0, 0], "y": [0, 0]})

            _m.assert_table_equal(t_0, t_1)
            _m.assert_table_equal(t_1, t_2)
            _m.assert_table_equal(t_2, t_3)

        with Stage("stage_1"):
            t_4 = _m.pd_dataframe({"x": [0, 0], "y": [0, 0]})
            t_5 = _m.noop(t_4)
            t_6 = _m.noop(t_5)
            t_7 = _m.noop(t_0)
            t_8 = _m.noop_sql(t_4)
            t_9 = _m.noop_sql(t_5)
            t_10 = _m.noop_sql(t_0)
            t_11 = _m.noop_lazy(t_4)
            t_12 = _m.noop_lazy(t_5)
            t_13 = _m.noop_lazy(t_0)
            if _m.pl is not None:
                t_14 = _m.noop_polars(t_4)
                t_15 = _m.noop_polars(t_5)
                t_16 = _m.noop_polars(t_0)
                t_17 = _m.noop_lazy_polars(t_4)
                t_18 = _m.noop_lazy_polars(t_5)
                t_19 = _m.noop_lazy_polars(t_0)

        with Stage("stage_2"):
            _m.assert_table_equal(t_0, t_4)
            _m.assert_table_equal(t_1, t_5)
            _m.assert_table_equal(t_2, t_6)
            _m.assert_table_equal(t_3, t_7)
            _m.assert_table_equal(t_1, t_8)
            _m.assert_table_equal(t_2, t_9)
            _m.assert_table_equal(t_3, t_10)
            _m.assert_table_equal(t_1, t_11)
            _m.assert_table_equal(t_2, t_12)
            _m.assert_table_equal(t_3, t_13)
            if _m.pl is not None:
                _m.assert_table_equal(t_1, t_14)
                _m.assert_table_equal(t_2, t_15)
                _m.assert_table_equal(t_3, t_16)
                _m.assert_table_equal(t_1, t_17)
                _m.assert_table_equal(t_2, t_18)
                _m.assert_table_equal(t_3, t_19)

    assert f.run().successful


def test_materialize_memo_with_failure():
    with Flow("flow") as f:
        with Stage("stage"):
            m.exception(1, False)
            m.exception(1, False)

            one = m.one()
            m.exception(1, True)
            t_3 = m.exception(one, True)
            m.exception(t_3, False)

    with ConfigContext.get().evolve(swallow_exceptions=True):
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


@pytest.mark.parametrize("imperative", [False, True])
def test_name_mangling_tables(imperative):
    @materialize
    def table_task_1():
        tbl = Table(pd.DataFrame({"x": [1]}), name="table_%%")
        return tbl.materialize() if imperative else tbl

    @materialize
    def table_task_2():
        tbl = Table(pd.DataFrame({"x": [2]}), name="table_%%")
        return tbl.materialize() if imperative else tbl

    with Flow() as f:
        with Stage("stage_1"):
            table_1 = table_task_1()
            table_2 = table_task_2()

    with StageLockContext():
        result = f.run()
        assert result.get(table_1, as_type=pd.DataFrame)["x"][0] == 1
        assert result.get(table_2, as_type=pd.DataFrame)["x"][0] == 2


@pytest.mark.parametrize("imperative", [False, True])
def test_name_mangling_lazy_tables(imperative):
    @materialize(lazy=True)
    def lazy_task_1():
        tbl = Table(select_as(1, "x"), name="table_%%")
        return tbl.materialize() if imperative else tbl

    @materialize(lazy=True)
    def lazy_task_2():
        tbl = Table(select_as(2, "x"), name="table_%%")
        return tbl.materialize() if imperative else tbl

    with Flow() as f:
        with Stage("stage_1"):
            lazy_1 = lazy_task_1()
            lazy_2 = lazy_task_2()

    with StageLockContext():
        result = f.run()
        assert result.get(lazy_1, as_type=pd.DataFrame)["x"][0] == 1
        assert result.get(lazy_2, as_type=pd.DataFrame)["x"][0] == 2


@pytest.mark.parametrize("imperative", [False, True])
def test_name_mangling_lazy_table_cache_fn(imperative):
    # Only the cache fn output of these two tasks is different
    @materialize(lazy=True, cache=lambda: 1, name="lazy_task")
    def lazy_task_1():
        tbl = Table(select_as(1, "x"))
        return tbl.materialize() if imperative else tbl

    @materialize(lazy=True, cache=lambda: 2, name="lazy_task")
    def lazy_task_2():
        tbl = Table(select_as(2, "x"))
        return tbl.materialize() if imperative else tbl

    with Flow() as f:
        with Stage("stage_1"):
            lazy_1 = lazy_task_1()
            lazy_2 = lazy_task_2()

    with StageLockContext():
        result = f.run()
        assert result.get(lazy_1, as_type=pd.DataFrame)["x"][0] == 1
        assert result.get(lazy_2, as_type=pd.DataFrame)["x"][0] == 2


def test_run_flow_with_empty_stage():
    with Flow("f") as f:
        with Stage("stage_0"):
            pass

    assert f.run().successful


@materialize(lazy=True, input_type=sa.Table)
def _check_nullable_task(tables):
    assert len(tables) == 18
    # This is dialect specific:
    # assert [c.nullable for c in tables[0].c] == [True, True, True]
    assert [c.nullable for c in tables[1].c] == [False, True, False]
    assert [c.nullable for c in tables[2].c] == [True, False, True]
    assert [c.nullable for c in tables[3].c] == [True, False, True]
    assert [c.nullable for c in tables[4].c] == [False, True, False]
    assert [c.nullable for c in tables[5].c] == [True, True, True]
    assert [c.nullable for c in tables[6].c] == [True, False, True]
    assert [c.nullable for c in tables[7].c] == [False, True, False]
    assert [c.nullable for c in tables[8].c] == [False, False, False]
    # assert [c.nullable for c in tables[9].c] == [True, True, True]
    assert [c.nullable for c in tables[10].c][1:] == [True, False]
    assert [c.nullable for c in tables[11].c][1:] == [False, True]
    assert [c.nullable for c in tables[12].c][1:] == [False, True]
    assert [c.nullable for c in tables[13].c][1:] == [True, False]
    assert [c.nullable for c in tables[14].c][1:] == [True, True]
    assert [c.nullable for c in tables[15].c][1:] == [False, True]
    assert [c.nullable for c in tables[16].c][1:] == [True, False]
    assert [c.nullable for c in tables[17].c][1:] == [False, False]
    return tables


def _test_nullable_lazy_get_flow():
    cols = sa.literal(1).label("x"), sa.literal(2).label("y"), sa.literal(3).label("z")

    @materialize(lazy=True)
    def lazy_task_1():
        q = sa.select(*cols)
        tables = [
            Table(q),
            Table(q, nullable=["y"]),
            Table(q, nullable=["x", "z"]),
            Table(q, non_nullable=["y"]),
            Table(q, non_nullable=["x", "z"]),
            Table(q, nullable=["x", "y", "z"], non_nullable=[]),
            Table(q, nullable=["x", "z"], non_nullable=["y"]),
            Table(q, nullable=["y"], non_nullable=["x", "z"]),
            Table(q, nullable=[], non_nullable=["x", "y", "z"]),
            Table(q, primary_key=["x"]),
            Table(q, primary_key=["x"], nullable=["y"]),
            Table(q, primary_key=["x"], nullable=["x", "z"]),
            Table(q, primary_key=["x"], non_nullable=["y"]),
            Table(q, primary_key=["x"], non_nullable=["x", "z"]),
            Table(q, primary_key=["x"], nullable=["x", "y", "z"], non_nullable=[]),
            Table(q, primary_key=["x"], nullable=["x", "z"], non_nullable=["y"]),
            Table(q, primary_key=["x"], nullable=["y"], non_nullable=["x", "z"]),
            Table(q, primary_key=["x"], nullable=[], non_nullable=["x", "y", "z"]),
        ]
        return tables

    with Flow() as f:
        with Stage("stage_1"):
            lazy_tables = lazy_task_1()
            _check_nullable_task(lazy_tables)
    return f, lazy_tables


def _test_nullable_get_flow():
    @materialize
    def task_1():
        df = pd.DataFrame({"x": [1], "y": [2], "z": [3]})
        tables = [
            Table(df),
            Table(df, nullable=["y"]),
            Table(df, nullable=["x", "z"]),
            Table(df, non_nullable=["y"]),
            Table(df, non_nullable=["x", "z"]),
            Table(df, nullable=["x", "y", "z"], non_nullable=[]),
            Table(df, nullable=["x", "z"], non_nullable=["y"]),
            Table(df, nullable=["y"], non_nullable=["x", "z"]),
            Table(df, nullable=[], non_nullable=["x", "y", "z"]),
            Table(df, primary_key=["x"]),
            Table(df, primary_key=["x"], nullable=["y"]),
            Table(df, primary_key=["x"], nullable=["x", "z"]),
            Table(df, primary_key=["x"], non_nullable=["y"]),
            Table(df, primary_key=["x"], non_nullable=["x", "z"]),
            Table(df, primary_key=["x"], nullable=["x", "y", "z"], non_nullable=[]),
            Table(df, primary_key=["x"], nullable=["x", "z"], non_nullable=["y"]),
            Table(df, primary_key=["x"], nullable=["y"], non_nullable=["x", "z"]),
            Table(df, primary_key=["x"], nullable=[], non_nullable=["x", "y", "z"]),
        ]
        return tables

    with Flow() as f:
        with Stage("stage_1"):
            lazy_tables = task_1()
            _check_nullable_task(lazy_tables)
    return f, lazy_tables


@skip_instances("snowflake")
@pytest.mark.parametrize(
    "_get_flow", [_test_nullable_get_flow, _test_nullable_lazy_get_flow]
)
def test_nullable(_get_flow):
    f, tables = _get_flow()

    with StageLockContext():
        result = f.run()
        assert result.successful
        for i in range(18):  # unfortunately, 'for tbl in tables' does not work
            tbl = tables[i]
            assert len(result.get(tbl, as_type=pd.DataFrame)) == 1
            assert result.get(tbl, as_type=pd.DataFrame)["x"][0] == 1
            assert result.get(tbl, as_type=pd.DataFrame)["y"][0] == 2
            assert result.get(tbl, as_type=pd.DataFrame)["z"][0] == 3
        # check that there are 9 tables with one primary key column each by checking
        # the sum of all primary key columns
        if ConfigContext.get().store.table_store.engine.dialect.name != "duckdb":
            assert (
                sum(
                    col.primary_key
                    for i in range(18)
                    for col in result.get(tables[i], as_type=sa.Table).original.c
                )
                == 9
            )


# duckdb does not persist nullable flags over connections; snowflake is slow
@skip_instances("duckdb", "snowflake")
@pytest.mark.parametrize(
    "_get_flow", [_test_nullable_get_flow, _test_nullable_lazy_get_flow]
)
def test_nullable_output(_get_flow):
    f, tables = _get_flow()

    def get_nullable(idx):
        return [c.nullable for c in result.get(tables[idx], as_type=sa.Table).c]

    with StageLockContext():
        result = f.run()
        assert result.successful
        # This is dialect specific:
        # assert get_nullable(0) == [True, True, True]
        assert get_nullable(1) == [False, True, False]
        assert get_nullable(2) == [True, False, True]
        assert get_nullable(3) == [True, False, True]
        assert get_nullable(4) == [False, True, False]
        assert get_nullable(5) == [True, True, True]
        assert get_nullable(6) == [True, False, True]
        assert get_nullable(7) == [False, True, False]
        assert get_nullable(8) == [False, False, False]


@pytest.mark.parametrize(
    "nullable, non_nullable, error",
    [
        (["a"], None, ValueError),
        (None, ["a"], ValueError),
        (["a", "x", "y", "z"], None, ValueError),
        (None, ["a", "x", "y", "z"], ValueError),
        (["x", "y", "z"], ["a"], ValueError),
        (["a"], ["x", "y", "z"], ValueError),
        (["x"], ["x"], ValueError),
        (["x"], ["y"], ValueError),
        (["x", "y"], ["y"], ValueError),
        ("x", None, TypeError),
        (None, "x", TypeError),
        (1, None, TypeError),
        (None, 2, TypeError),
    ],
)
def test_nullable_raises(nullable, non_nullable, error):
    cols = sa.literal(1).label("x"), sa.literal(2).label("y"), sa.literal(3).label("z")

    @materialize(lazy=True)
    def lazy_task_1():
        q = sa.select(*cols)
        df = pd.DataFrame({"x": [1], "y": [2], "z": [3]})
        tables = [
            Table(q, nullable=nullable, non_nullable=non_nullable),
            Table(df, nullable=nullable, non_nullable=non_nullable),
            Table(q, primary_key=["x"], nullable=nullable, non_nullable=non_nullable),
            Table(df, primary_key=["x"], nullable=nullable, non_nullable=non_nullable),
        ]
        return tables

    with Flow() as f:
        with Stage("stage_1"):
            lazy_task_1()

    with pytest.raises(error):
        f.run()


@materialize(lazy=True)
def _lazy_task_1():
    return Table(select_as(1, "x"), name="t1", primary_key="x")


@materialize(lazy=True, nout=2)
def _lazy_task_2():
    return Table(select_as(1, "x"), name="t21", indexes=[["x"]]), Table(
        select_as(2, "x"), name="t22", primary_key=["x"]
    )


@materialize(lazy=True, input_type=sa.Table)
def _lazy_join(src1: sa.sql.expression.Alias, src2: sa.sql.expression.Alias):
    query = sa.select(src1.c.x, src2.c.x.label("x2")).select_from(
        src1.outerjoin(src2, src1.c.x == src2.c.x)
    )
    return Table(query, "t3_%%", indexes=[["x2"], ["x", "x2"]])


@materialize(version="1.0")
def _sql_task_1():
    return Table(select_as(1, "x"), name="t1", primary_key="x")


@materialize(version="1.0", nout=2)
def _sql_task_2():
    return (
        Table(select_as(1, "x"), name="t21", indexes=[["x"]]),
        Table(select_as(2, "x"), name="t22", primary_key=["x"]),
    )


@materialize(version="1.0", input_type=sa.Table)
def _sql_join(src1: sa.sql.expression.Alias, src2: sa.sql.expression.Alias):
    query = sa.select(src1.c.x, src2.c.x.label("x2")).select_from(
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
    return (
        Table(df1, name="t21", indexes=[["x"]]),
        Table(df2, name="t22", primary_key=["x"]),
    )


@materialize(version="1.0", input_type=pd.DataFrame)
def _eager_join(src1: pd.DataFrame, src2: pd.DataFrame):
    src2["x2"] = src2["x"]
    join = src1.merge(src2, on="x", how="left")
    return Table(join, "t3_%%", indexes=[["x2"], ["x", "x2"]])


@pytest.mark.skip
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
    logger = structlog.get_logger("test_task_and_stage_communication")
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
