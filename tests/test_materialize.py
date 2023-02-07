from __future__ import annotations

import pandas as pd
import pytest

from pydiverse.pipedag import Flow, Stage, Table, materialize
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

            materialize_and_retrieve(3.14)
            materialize_and_retrieve(-2.71)

            materialize_and_retrieve("")
            materialize_and_retrieve("a string")

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

        with Stage("stage_2"):
            m.assert_table_equal(t_0, t_4)
            m.assert_table_equal(t_1, t_5)
            m.assert_table_equal(t_2, t_6)
            m.assert_table_equal(t_3, t_7)

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
