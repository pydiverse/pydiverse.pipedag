from __future__ import annotations

import pandas as pd

from pydiverse.pipedag import Blob, ConfigContext, Flow, Stage, Table
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.materialize.container import RawSql
from pydiverse.pipedag.materialize.core import materialize

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import ALL_INSTANCES, with_instances
from tests.util import compile_sql, select_as
from tests.util import tasks_library as m
from tests.util.spy import spy_task

pytestmark = [with_instances(ALL_INSTANCES)]


# Test Basic Cache Invalidation Behaviour


def test_change_bound_argument(mocker):
    input_list = [1]

    with Flow() as flow:
        with Stage("stage_1"):
            out = m.noop(input_list)
            child = m.noop2(out)

    # Initial Call
    with StageLockContext():
        result = flow.run()
        assert result.get(out)[0] == 1
        assert result.get(child)[0] == 1

    # Calling flow.run again shouldn't call the task
    out_spy = spy_task(mocker, out)
    child_spy = spy_task(mocker, child)
    with StageLockContext():
        result = flow.run()
        assert result.get(out)[0] == 1
        assert result.get(child)[0] == 1
        out_spy.assert_not_called()
        child_spy.assert_not_called()

    # Changing the input object should invalidate the cache
    input_list[0] = 2
    with StageLockContext():
        result = flow.run()
        assert result.get(out)[0] == 2
        assert result.get(child)[0] == 2
        out_spy.assert_called_once()
        child_spy.assert_called_once()


def test_changed_cache_fn_literal(mocker):
    cache_value = 0

    def cache():
        return cache_value

    @materialize(cache=cache, version="1.0")
    def return_cache_value():
        return cache_value

    with Flow() as flow:
        with Stage("stage_1"):
            out = return_cache_value()
            child = m.noop(out)

    # Initial Call
    with StageLockContext():
        result = flow.run()
        assert result.get(out) == 0
        assert result.get(child) == 0

    # Calling flow.run again shouldn't call the task
    out_spy = spy_task(mocker, out)
    child_spy = spy_task(mocker, child)
    with StageLockContext():
        result = flow.run()
        assert result.get(out) == 0
        assert result.get(child) == 0
        out_spy.assert_not_called()
        child_spy.assert_not_called()

    # Changing the cache value should cause it to get called again
    cache_value = 1
    with StageLockContext():
        result = flow.run()
        assert result.get(out) == 1
        assert result.get(child) == 1
        out_spy.assert_called_once()
        child_spy.assert_called_once()


def test_change_cache_fn_table(mocker):
    cache_value = 0

    def cache():
        return cache_value

    @materialize(cache=cache, version="1.0")
    def return_cache_table():
        return Table(select_as(cache_value, "x"))

    @materialize(input_type=pd.DataFrame, version="1.0")
    def get_first(table, col):
        return int(table[col][0])

    with Flow() as flow:
        with Stage("stage_1"):
            out = return_cache_table()
            child = get_first(out, "x")

    # Initial Call
    with StageLockContext():
        result = flow.run()
        assert result.get(child) == 0

    # Calling flow.run again shouldn't call the task
    out_spy = spy_task(mocker, out)
    child_spy = spy_task(mocker, child)
    with StageLockContext():
        result = flow.run()
        assert result.get(child) == 0
        out_spy.assert_not_called()
        child_spy.assert_not_called()

    # Changing the cache value should cause it to get called again
    cache_value = 1
    with StageLockContext():
        result = flow.run()
        assert result.get(child) == 1
        out_spy.assert_called_once()
        child_spy.assert_called_once()


def test_change_cache_fn_blob(mocker):
    cache_value = 0

    def cache():
        return cache_value

    @materialize(cache=cache, version="1.0")
    def return_cache_blob():
        return Blob(cache_value)

    with Flow() as flow:
        with Stage("stage_1"):
            out = return_cache_blob()
            child = m.noop(out)

    # Initial Call
    with StageLockContext():
        result = flow.run()
        assert result.get(out) == 0
        assert result.get(child) == 0

    # Calling flow.run again shouldn't call the task
    out_spy = spy_task(mocker, out)
    child_spy = spy_task(mocker, child)
    with StageLockContext():
        result = flow.run()
        assert result.get(out) == 0
        assert result.get(child) == 0
        out_spy.assert_not_called()
        child_spy.assert_not_called()

    # Changing the cache value should cause it to get called again
    cache_value = 1
    with StageLockContext():
        result = flow.run()
        assert result.get(out) == 1
        assert result.get(child) == 1
        out_spy.assert_called_once()
        child_spy.assert_called_once()


def test_change_task_version_literal(mocker):
    with Flow() as flow:
        with Stage("stage_1"):
            out = m.one()
            child = m.noop(out)

    # Initial Call
    out.version = "VERSION 0"
    assert flow.run().successful

    # Second Call (Should be cached)
    out_spy = spy_task(mocker, out)
    child_spy = spy_task(mocker, child)
    assert flow.run().successful
    out_spy.assert_not_called()
    child_spy.assert_not_called()

    # Changing the version should invalidate the cache, but the child still
    # shouldn't get called because the parent task still returned the same value.
    out.version = "VERSION 1"
    assert flow.run().successful
    out_spy.assert_called_once()
    child_spy.assert_not_called()


def test_change_task_version_table(mocker):
    with Flow() as flow:
        with Stage("stage_1"):
            out = m.simple_dataframe()
            child = m.noop(out)
            child2 = m.noop_sql(out)  # lazy=False task
            child_lazy = m.noop_lazy(out)  # lazy=True task

    # Initial Call
    out.version = "VERSION 0"
    assert flow.run().successful

    # Second Call (Should be cached)
    out_spy = spy_task(mocker, out)
    child_spy = spy_task(mocker, child)
    child2_spy = spy_task(mocker, child2)
    child_lazy_spy = spy_task(mocker, child_lazy)
    assert flow.run().successful
    out_spy.assert_not_called()
    child_spy.assert_not_called()
    child2_spy.assert_not_called()
    child_lazy_spy.assert_called_once()

    # Changing the version should invalidate the cache. This should also invalidate
    # the child task because it receives the table as input.
    out.version = "VERSION 1"
    assert flow.run().successful
    out_spy.assert_called_once()
    child_spy.assert_called_once()
    child2_spy.assert_called_once()
    child_lazy_spy.assert_called_once()


def test_change_task_version_blob(mocker):
    with Flow() as flow:
        with Stage("stage_1"):
            out = m.object_blob({"x": "y"})
            child = m.as_blob(out)

    # Initial Call
    out.version = "VERSION 0"
    assert flow.run().successful

    # Second Call (Should be cached)
    out_spy = spy_task(mocker, out)
    child_spy = spy_task(mocker, child)
    assert flow.run().successful
    out_spy.assert_not_called()
    child_spy.assert_not_called()

    # Changing the version should invalidate the cache. This should also invalidate
    # the child task because it receives the blob as input.
    out.version = "VERSION 1"
    assert flow.run().successful
    out_spy.assert_called_once()
    child_spy.assert_called_once()


def test_change_lazy_query(mocker):
    query_value = 1

    @materialize(lazy=True, nout=2)
    def lazy_task():
        return 0, Table(select_as(query_value, "x"), name="lazy_table")

    @materialize(input_type=pd.DataFrame, version="1.0")
    def get_first(table, col):
        return int(table[col][0])

    with Flow() as flow:
        with Stage("stage_1"):
            const, lazy = lazy_task()
            value = get_first(lazy, "x")
            const2 = m.noop(const)
            lazy2 = m.noop_sql(lazy)  # lazy=False task
            lazy3 = m.noop_lazy(lazy)

    # Initial Run
    lazy_spy = spy_task(mocker, lazy)
    with StageLockContext():
        result = flow.run()
        assert result.successful
        assert result.get(value) == 1
        lazy_spy.assert_called_once()

    # Second run, because the task is lazy, it should always get called.
    # The value task however shouldn't get called.
    value_spy = spy_task(mocker, value)
    const2_spy = spy_task(mocker, const2)
    lazy2_spy = spy_task(mocker, lazy2)
    lazy3_spy = spy_task(mocker, lazy3)
    with StageLockContext():
        result = flow.run()
        assert result.successful
        assert result.get(value) == 1
        lazy_spy.assert_called_once()
        value_spy.assert_not_called()
        const2_spy.assert_not_called()
        lazy2_spy.assert_not_called()
        lazy3_spy.assert_called_once()

    # Third run with changed query_value
    query_value = 2
    with StageLockContext():
        result = flow.run()
        assert result.successful
        assert result.get(value) == 2
        lazy_spy.assert_called_once()
        value_spy.assert_called_once()
        const2_spy.assert_not_called()
        lazy2_spy.assert_called_once()
        lazy3_spy.assert_called_once()


def test_change_raw_sql(mocker):
    query_value = 1

    @materialize(lazy=True, nout=2, version="1.0")
    def raw_task(stage):
        raw_sql = compile_sql(select_as(query_value, "x"))
        return 0, RawSql(raw_sql, "raw_task")

    @materialize(version="1.0")
    def raw_child(raw):
        return raw.sql

    with Flow() as flow:
        with Stage("stage_1") as s:
            const, raw = raw_task(s)
            child = raw_child(raw)
            const = m.noop(const)

    # Initial Run
    raw_spy = spy_task(mocker, raw)
    with StageLockContext():
        result = flow.run()
        assert result.successful
        raw_spy.assert_called_once()

    # Second run, because the task is lazy, it should always get called.
    # The value task however shouldn't get called.
    child_spy = spy_task(mocker, child)
    const_spy = spy_task(mocker, const)
    with StageLockContext():
        result = flow.run()
        assert result.successful
        assert "SELECT 1" in result.get(child).upper()
        raw_spy.assert_called_once()
        child_spy.assert_not_called()
        const_spy.assert_not_called()

    # Third run with changed query_str
    query_value = 2
    with StageLockContext():
        result = flow.run()
        assert result.successful
        assert "SELECT 2" in result.get(child).upper()
        raw_spy.assert_called_once()
        child_spy.assert_called_once()
        const_spy.assert_not_called()


def test_change_task_stage_literal(mocker):
    with Flow() as flow:
        with Stage("stage_1"):
            one = m.one()
        with Stage("stage_2"):
            m.noop(0)  # This is to clear the stage
        with Stage("stage_3"):
            child = m.noop(one)
    _ = child

    assert flow.run().successful

    with Flow() as flow:
        with Stage("stage_2"):
            one = m.one()
        with Stage("stage_3"):
            child = m.noop(one)

    one_spy = spy_task(mocker, one)
    child_spy = spy_task(mocker, child)

    # Moving the one task to a different stage should cause it to be called again,
    # but because its return value is the same, the child task shouldn't get called.
    assert flow.run().successful
    one_spy.assert_called_once()
    child_spy.assert_not_called()


def test_change_task_stage_table(mocker):
    with Flow() as flow:
        with Stage("stage_1"):
            table = m.simple_dataframe()
        with Stage("stage_2"):
            m.noop(0)  # This is to clear the stage
        with Stage("stage_3"):
            child = m.noop(table)

    assert flow.run().successful

    with Flow() as flow:
        with Stage("stage_2"):
            table = m.simple_dataframe()
        with Stage("stage_3"):
            child = m.noop(table)

    table_spy = spy_task(mocker, table)
    child_spy = spy_task(mocker, child)

    # Moving the table task to a different stage should cause it to be called again,
    # and because it returns a table, any child task should also get invalidated.
    assert flow.run().successful
    table_spy.assert_called_once()
    child_spy.assert_called_once()


def test_change_task_stage_blob(mocker):
    with Flow() as flow:
        with Stage("stage_1"):
            blob = m.as_blob(1)
        with Stage("stage_2"):
            m.noop(0)  # This is to clear the stage
        with Stage("stage_3"):
            child = m.noop(blob)

    assert flow.run().successful

    with Flow() as flow:
        with Stage("stage_2"):
            blob = m.as_blob(1)
        with Stage("stage_3"):
            child = m.noop(blob)

    blob_spy = spy_task(mocker, blob)
    child_spy = spy_task(mocker, child)

    # Moving the blob task to a different stage should cause it to be called again,
    # and because it returns a blob, any child task should also get invalidated.
    assert flow.run().successful
    blob_spy.assert_called_once()
    child_spy.assert_called_once()


def test_different_task_same_input(mocker):
    with Flow() as flow:
        with Stage("stage_1"):
            one = m.one()
            two = m.two()

    # Initial run
    with StageLockContext():
        result = flow.run()
        assert result.successful
        assert result.get(one) == 1
        assert result.get(two) == 2

    # Second Run should be cached
    one_spy = spy_task(mocker, one)
    two_spy = spy_task(mocker, two)

    with StageLockContext():
        result = flow.run()
        assert result.successful
        assert result.get(one) == 1
        assert result.get(two) == 2
        one_spy.assert_not_called()
        two_spy.assert_not_called()


def test_same_task_different_stages(mocker):
    with Flow() as flow:
        with Stage("stage_1"):
            out_s1 = m.one()
        with Stage("stage_2"):
            out_s2 = m.one()

    # Initial run
    with StageLockContext():
        result = flow.run()
        assert result.successful
        assert result.get(out_s1) == 1
        assert result.get(out_s2) == 1

    # Second Run should be cached
    out_spy = spy_task(mocker, out_s1)

    with StageLockContext():
        result = flow.run()
        assert result.successful
        assert result.get(out_s1) == 1
        assert result.get(out_s2) == 1
        out_spy.assert_not_called()


def test_change_version_table(mocker):
    cache_value = 0

    def cache():
        return cache_value

    @materialize(lazy=True, cache=cache)
    def return_cache_table():
        return Table(select_as(cache_value, "x"))

    def get_flow(version):
        @materialize(version=version, input_type=pd.DataFrame)
        def named_copy(table: pd.DataFrame):
            return Table(table, "_table_copy")

        with Flow() as flow:
            with Stage("stage_1"):
                out = return_cache_table()
                cpy = named_copy(out)
        return flow, out, cpy

    flow, out, cpy = get_flow(version="1.0")
    # Initial Call
    with StageLockContext():
        result = flow.run()
        assert result.get(cpy)["x"].iloc[0] == 0

    # Calling flow.run again shouldn't call the task
    out_spy = spy_task(mocker, out)
    cpy_spy = spy_task(mocker, cpy)
    with StageLockContext():
        result = flow.run()
        assert result.get(cpy)["x"].iloc[0] == 0
        out_spy.assert_called_once()  # lazy task is always called
        cpy_spy.assert_not_called()

    # Changing the cache value should cause it to get called again
    cache_value = 1
    with StageLockContext():
        result = flow.run()
        assert result.get(cpy)["x"].iloc[0] == 1
        out_spy.assert_called_once()
        cpy_spy.assert_called_once()

    # recreating flow without version change should not invalidate cache
    flow, out, cpy = get_flow(version="1.0")
    out_spy = spy_task(mocker, out)
    cpy_spy = spy_task(mocker, cpy)
    with StageLockContext():
        result = flow.run()
        assert result.get(cpy)["x"].iloc[0] == 1
        out_spy.assert_called_once()  # lazy task is always called
        cpy_spy.assert_not_called()

    # Changing the version value should cause cpy to get recreated again
    flow, out, cpy = get_flow(version="1.1")
    out_spy = spy_task(mocker, out)
    cpy_spy = spy_task(mocker, cpy)
    with StageLockContext():
        result = flow.run()
        assert result.get(cpy)["x"].iloc[0] == 1
        out_spy.assert_called_once()  # lazy task is always called
        cpy_spy.assert_called_once()

    # Changing the version value should cause cpy to get recreated again
    for _ in range(3):
        flow, out, cpy = get_flow(version=None)
        out_spy = spy_task(mocker, out)
        cpy_spy = spy_task(mocker, cpy)
        with StageLockContext():
            result = flow.run()
            assert result.get(cpy)["x"].iloc[0] == 1
            out_spy.assert_called_once()  # lazy task is always called
            cpy_spy.assert_called_once()


def test_ignore_task_version(mocker):
    cfg = ConfigContext.get().evolve(ignore_task_version=True)

    with Flow() as flow:
        with Stage("stage_1"):
            out = m.noop([1])
            child = m.noop2(out)

    # Initial Call
    with StageLockContext():
        result = flow.run(config=cfg)
        assert result.get(out)[0] == 1
        assert result.get(child)[0] == 1

    # Calling flow.run again should still call the task (disabled caching)
    out_spy = spy_task(mocker, out)
    child_spy = spy_task(mocker, child)
    for _ in range(3):
        with StageLockContext():
            result = flow.run(config=cfg)
            assert result.get(out)[0] == 1
            assert result.get(child)[0] == 1
            out_spy.assert_called_once()
            child_spy.assert_called_once()
