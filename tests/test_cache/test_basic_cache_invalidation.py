from __future__ import annotations

import pandas as pd
import pytest
import sqlalchemy as sa

from pydiverse.pipedag import Blob, ConfigContext, Flow, Stage, Table
from pydiverse.pipedag.container import RawSql
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.context.context import CacheValidationMode
from pydiverse.pipedag.materialize.core import (
    AUTO_VERSION,
    input_stage_versions,
    materialize,
)

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import ALL_INSTANCES, skip_instances, with_instances
from tests.util import compile_sql, select_as
from tests.util import tasks_library as m
from tests.util import tasks_library_imperative as m2
from tests.util.spy import spy_task
from tests.util.tasks_library import get_task_logger

try:
    import polars as pl
except ImportError:
    pl = None

# snowflake tests are too slow, possibly they could move to nightly tests
pytestmark = [with_instances(tuple(set(ALL_INSTANCES) - {"snowflake"}))]


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


@pytest.mark.parametrize("imperative", [False, True])
def test_change_cache_fn_table(mocker, imperative):
    cache_value = 0

    def cache():
        return cache_value

    @materialize(cache=cache, version="1.0")
    def return_cache_table():
        tbl = Table(select_as(cache_value, "x"))
        return tbl.materialize() if imperative else tbl

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


@with_instances("postgres")
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
    out._version = "VERSION 0"
    assert flow.run().successful

    # Second Call (Should be cached)
    out_spy = spy_task(mocker, out)
    child_spy = spy_task(mocker, child)
    assert flow.run().successful
    out_spy.assert_not_called()
    child_spy.assert_not_called()

    # Changing the version should invalidate the cache, but the child still
    # shouldn't get called because the parent task still returned the same value.
    out._version = "VERSION 1"
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
    out._version = "VERSION 0"
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
    out._version = "VERSION 1"
    assert flow.run().successful
    out_spy.assert_called_once()
    child_spy.assert_called_once()
    child2_spy.assert_called_once()
    child_lazy_spy.assert_called_once()


@with_instances("postgres")
def test_change_task_version_blob(mocker):
    with Flow() as flow:
        with Stage("stage_1"):
            out = m.object_blob({"x": "y"})
            child = m.as_blob(out)

    # Initial Call
    out._version = "VERSION 0"
    assert flow.run().successful

    # Second Call (Should be cached)
    out_spy = spy_task(mocker, out)
    child_spy = spy_task(mocker, child)
    assert flow.run().successful
    out_spy.assert_not_called()
    child_spy.assert_not_called()

    # Changing the version should invalidate the cache. This should also invalidate
    # the child task because it receives the blob as input.
    out._version = "VERSION 1"
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


@with_instances("postgres")
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


@pytest.mark.parametrize("ignore_task_version", [True, False])
@pytest.mark.parametrize("disable_cache_function", [True, False])
@pytest.mark.parametrize("mode", ["ASSERT_NO_FRESH_INPUT", "NORMAL"])
def test_cache_validation_mode_assert(
    ignore_task_version, disable_cache_function, mode
):
    mode = getattr(CacheValidationMode, mode.upper())
    kwargs = dict(
        ignore_task_version=ignore_task_version,
        disable_cache_function=disable_cache_function,
        cache_validation_mode=mode,
    )
    if not disable_cache_function and mode == CacheValidationMode.NORMAL:
        pytest.skip("Combination does not provoke exception")
        return
    error = (
        AssertionError
        if mode == CacheValidationMode.ASSERT_NO_FRESH_INPUT
        else ValueError
    )

    cache_value = 0
    return_value = 0

    def cache():
        return cache_value

    def get_flow(version):
        @materialize(version=version, cache=cache)
        def source():
            return pd.DataFrame(dict(x=[return_value]))

        @materialize(lazy=True, cache=cache)
        def lazy_source():
            return Table(select_as(return_value, "x"))

        with Flow() as flow:
            with Stage("stage_1"):
                s1 = source()
                s2 = lazy_source()
        return flow, s1, s2

    flow, s1, s2 = get_flow(version="1.0")
    # Initial Call
    with StageLockContext():
        result = flow.run(
            cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID,
            disable_cache_function=False,
            ignore_task_version=False,
        )
        assert result.successful
        assert result.get(s1, as_type=pd.DataFrame)["x"].iloc[0] == return_value
        assert result.get(s2, as_type=pd.DataFrame)["x"].iloc[0] == return_value

    if disable_cache_function or ignore_task_version:
        with pytest.raises(error):
            result = flow.run(**kwargs)
            assert result.successful

    cache_value = 1  # IGNORE_FRESH_INPUT should be implied
    if (
        not disable_cache_function
        and not ignore_task_version
        and mode != CacheValidationMode.NORMAL
    ):
        result = flow.run(
            cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID,
            disable_cache_function=False,
            ignore_task_version=False,
        )
        assert result.successful

    return_value = 1
    with pytest.raises(error):
        flow.run(**kwargs)

    return_value = 0
    flow, _, _ = get_flow(version="1.1")
    with pytest.raises(error):
        flow.run(**kwargs)


@with_instances("postgres", "local_table_cache")
@pytest.mark.parametrize("ignore_task_version", [True, False])
@pytest.mark.parametrize("disable_cache_function", [True, False])
@pytest.mark.parametrize(
    "mode", ["NORMAL", "IGNORE_FRESH_INPUT", "FORCE_FRESH_INPUT", "FORCE_CACHE_INVALID"]
)
@pytest.mark.parametrize("imperative", [False, True])
def test_cache_validation_mode(
    ignore_task_version, disable_cache_function, mode, imperative, mocker
):
    _m = m2 if imperative else m
    mode = getattr(CacheValidationMode, mode.upper())
    if disable_cache_function and mode == CacheValidationMode.NORMAL:
        pytest.skip("Cannot disable cache function in mode NORMAL")
        return

    kwargs = dict(
        ignore_task_version=ignore_task_version,
        disable_cache_function=disable_cache_function,
        cache_validation_mode=mode,
    )
    cache_calls = [0]  # consider using mocker also for this
    cache_value = 0

    def cache():
        cache_calls[0] += 1
        return cache_value

    def cache2(df):
        cache_calls[0] += 1
        return cache_value

    @materialize(lazy=True, cache=cache)
    def return_cache_table_lazy():
        tbl = Table(select_as(cache_value, "x"))
        return tbl.materialize() if imperative else tbl

    @materialize(version=None, cache=cache)
    def return_cache_table_always():
        df = pd.DataFrame(dict(x=[cache_value]))
        return Table(df).materialize() if imperative else df

    @materialize(version=AUTO_VERSION, cache=cache2, input_type=pd.DataFrame)
    def return_cache_table_auto(df: pd.DataFrame):
        return Table(df).materialize() if imperative else df

    @materialize(version="1.0", cache=cache)
    def return_cache_table():
        df = pd.DataFrame(dict(x=[cache_value]))
        return Table(df).materialize() if imperative else df

    @input_stage_versions(lazy=True, input_type=sa.Table)
    def dummy_copy_inputs(
        transaction: dict[str, sa.sql.expressions.Alias],
        other: dict[str, sa.sql.expressions.Alias],
    ):
        _ = other  # we cannot make any assumptions on the other stage version
        assert len(transaction) == 0
        return Table(sa.select(sa.literal(1).label("a")), name="dummy")

    @input_stage_versions(lazy=True, input_type=sa.Table)
    def validate_stage(
        transaction: dict[str, sa.sql.expressions.Alias],
        other: dict[str, sa.sql.expressions.Alias],
    ):
        _ = other  # we cannot make any assumptions on the other stage version
        get_task_logger().info(f"Transaction tables: {transaction}")
        transaction_schema = {tbl.original.schema for tbl in transaction.values()} - {
            tbl.original.schema for tbl in other.values()
        }
        assert transaction_schema
        assert transaction_schema.pop().startswith(
            {tbl.original.schema for tbl in other.values()}.pop().split("__")[0]
        )
        assert (
            len([tbl for tbl in transaction.keys() if not tbl.endswith("__copy")]) == 12
        )

    @input_stage_versions(lazy=True, input_type=sa.Table)
    def validate_stage2(
        transaction: dict[str, sa.sql.expressions.Alias],
        other: dict[str, sa.sql.expressions.Alias],
    ):
        # it is expected that we have a "Failed to retrieve"-exception in other stage
        _ = other
        assert len(transaction) == 1

    def get_flow():
        with Flow() as flow:
            with Stage("stage_1"):
                x = dummy_copy_inputs()
                _ = m.noop(x)
                out_lazy = return_cache_table_lazy()
                out_always = return_cache_table_always()
                out_auto = return_cache_table_auto(out_lazy)
                out = return_cache_table()
                outs = [out_lazy, out_always, out_auto, out]
                cpy = [_m.noop(o) for o in outs]
                ind1 = _m.simple_dataframe()
                ind2 = _m.simple_lazy_table()
                validate_stage()
                validate_stage2(ind2)
        return flow, outs, cpy, [ind1, ind2]

    flow, outs, cpy, ind = get_flow()
    # Initial Call
    with StageLockContext():
        result = flow.run(
            cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID,
            disable_cache_function=False,
            ignore_task_version=False,
        )
        assert all(result.get(c, as_type=pd.DataFrame)["x"].iloc[0] == 0 for c in cpy)
    assert cache_calls[0] == 4
    cache_calls[0] = 0

    # Calling flow.run again
    out_spy = [spy_task(mocker, o) for o in outs]
    cpy_spy = [spy_task(mocker, c) for c in cpy]
    ind_spy = [spy_task(mocker, i) for i in ind]
    with StageLockContext():
        result = flow.run(**kwargs)
        assert all(result.get(c, as_type=pd.DataFrame)["x"].iloc[0] == 0 for c in cpy)
        out_spy[0].assert_called_once()  # lazy task is always called
        out_spy[1].assert_called_once()  # version=None task is always called
        if (
            mode
            in [
                CacheValidationMode.FORCE_FRESH_INPUT,
                CacheValidationMode.FORCE_CACHE_INVALID,
            ]
            and not disable_cache_function
        ):
            out_spy[2].assert_called(2)
        else:
            out_spy[2].assert_called_once()  # version=AUTO_VERSION task is called once
        if (
            mode
            not in [
                CacheValidationMode.FORCE_FRESH_INPUT,
                CacheValidationMode.FORCE_CACHE_INVALID,
            ]
            and not ignore_task_version
        ):
            out_spy[3].assert_not_called()
        else:
            out_spy[3].assert_called_once()
        if mode not in [CacheValidationMode.FORCE_CACHE_INVALID]:
            if ignore_task_version:
                all(spy.assert_called_once() for spy in cpy_spy)
            elif disable_cache_function:
                if mode == CacheValidationMode.IGNORE_FRESH_INPUT:
                    cpy_spy[0].assert_not_called()  # lazy cache_fn_hash is loaded
                    cpy_spy[1].assert_called_once()  # version=None is always called
                    cpy_spy[2].assert_not_called()  # cache_key is loaded
                    cpy_spy[3].assert_not_called()  # cache_key is loaded
                else:
                    all(spy.assert_called_once() for spy in cpy_spy)
            else:
                cpy_spy[0].assert_not_called()
                cpy_spy[1].assert_called_once()  # version=None is always called
                cpy_spy[2].assert_not_called()  # cache_fn_hash is stable
                cpy_spy[3].assert_not_called()
            if ignore_task_version:
                ind_spy[0].assert_called_once()
            else:
                ind_spy[0].assert_not_called()
            ind_spy[1].assert_called_once()  # lazy is always called
        else:
            all(spy.assert_called_once() for spy in cpy_spy)
            all(spy.assert_called_once() for spy in ind_spy)
    if disable_cache_function:
        assert cache_calls[0] == 0
    else:
        assert cache_calls[0] == 4

    # Changing the cache value should cause it to get called again
    cache_value = 1
    with StageLockContext():
        result = flow.run(**kwargs)
        if mode == CacheValidationMode.IGNORE_FRESH_INPUT and not ignore_task_version:
            assert all(
                result.get(c, as_type=pd.DataFrame)["x"].iloc[0] == res
                for c, res in zip(cpy, [cache_value, cache_value, cache_value, 0])
            )
        else:
            assert all(
                result.get(c, as_type=pd.DataFrame)["x"].iloc[0] == cache_value
                for c in cpy
            )
        out_spy[0].assert_called_once()  # lazy task is always called
        out_spy[1].assert_called_once()  # version=None task is always called
        if disable_cache_function and mode in [
            CacheValidationMode.FORCE_FRESH_INPUT,
            CacheValidationMode.FORCE_CACHE_INVALID,
        ]:
            out_spy[2].assert_called_once()
        else:
            out_spy[2].assert_called(2)  # version=AUTO_VERSION task is called twice
        if mode == CacheValidationMode.IGNORE_FRESH_INPUT and not ignore_task_version:
            out_spy[3].assert_not_called()
        else:
            out_spy[3].assert_called_once()
        if mode not in [CacheValidationMode.FORCE_CACHE_INVALID]:
            all(spy.assert_called_once() for spy in cpy_spy)
            if ignore_task_version:
                ind_spy[0].assert_called_once()
            else:
                ind_spy[0].assert_not_called()
            ind_spy[1].assert_called_once()  # lazy is always called
        else:
            all(spy.assert_called_once() for spy in cpy_spy)
            all(spy.assert_called_once() for spy in ind_spy)


@skip_instances("postgres", "local_table_cache", "snowflake")
@pytest.mark.parametrize("ignore_task_version", [False])
@pytest.mark.parametrize("disable_cache_function", [False])
@pytest.mark.parametrize(
    "mode", ["NORMAL", "IGNORE_FRESH_INPUT", "FORCE_FRESH_INPUT", "FORCE_CACHE_INVALID"]
)
@pytest.mark.parametrize("imperative", [True])
def test_cache_validation_mode_reduced(
    ignore_task_version, disable_cache_function, mode, imperative, mocker
):
    # Reduce combinatorial space for duckdb to avoid timeout after 10min
    # duckdb is particularly slow for those tests (~10x: 7-15s instead of 1-2s).
    # This reduced combinatorial space is even too much for snowflake. There,
    # we have 1-2 seconds roundtrip time for many individual database requests.
    # And even just reflecting a SQLAlchemy tables issues several of those.
    # Thus this test takes >30min alone for snowflake. In realistic scenarios,
    # the 1-2s per request should not matter too much, though.
    test_cache_validation_mode(
        ignore_task_version, disable_cache_function, mode, imperative, mocker
    )


@pytest.mark.parametrize("n", [1, 2, 15])
def test_partial_stage_cache_valid(mocker, n):
    cache_value = 0

    def cache():
        return cache_value

    @materialize(lazy=True, cache=cache)
    def return_cache_table():
        return Table(select_as(cache_value, "x"))

    @materialize(version="1.0")
    def n_tables(n):
        return [pd.DataFrame(dict(x=[i])) for i in range(n)]

    @materialize(lazy=True)
    def n_lazy_tables(n):
        return [Table(select_as(i, "x")) for i in range(n)]

    def get_flow(version):
        @materialize(version=version, input_type=pd.DataFrame)
        def named_copy(table: pd.DataFrame):
            return Table(table, "_table_copy")

        with Flow() as flow:
            with Stage("stage_1"):
                # these tasks are cache valid in all but the first call:
                tbls = n_tables(n)
                lazy_tbls = n_lazy_tables(n)
                # these tables may be cache invalidated by cache_value
                out = return_cache_table()
                cpy = named_copy(out)
        return flow, out, cpy, tbls, lazy_tbls

    flow, out, cpy, tbls, lazy_tbls = get_flow(version="1.0")
    # Initial Call
    with StageLockContext():
        result = flow.run()
        assert result.get(cpy)["x"].iloc[0] == 0

    # Calling flow.run again shouldn't call the task
    out_spy = spy_task(mocker, out)
    cpy_spy = spy_task(mocker, cpy)
    tbls_spy = spy_task(mocker, tbls)
    lazy_tbls_spy = spy_task(mocker, lazy_tbls)
    with StageLockContext():
        result = flow.run()
        assert result.get(cpy)["x"].iloc[0] == 0
        out_spy.assert_called_once()  # lazy task is always called
        cpy_spy.assert_not_called()
        tbls_spy.assert_not_called()
        lazy_tbls_spy.assert_called_once()  # lazy task is always called

    # Changing the cache value should cause it to get called again
    # In this case tbls and lazy_tbls are still cache valid and thus need to be
    # copied over to transaction schema
    cache_value = 1
    with StageLockContext():
        result = flow.run()
        assert result.get(cpy)["x"].iloc[0] == 1
        out_spy.assert_called_once()
        cpy_spy.assert_called_once()
        tbls_spy.assert_not_called()
        lazy_tbls_spy.assert_called_once()  # lazy task is always called


def test_ignore_task_version(mocker):
    cfg = ConfigContext.get().evolve(cache_validation=dict(ignore_task_version=True))

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


@pytest.mark.polars
def test_lazy_table_without_query_string(mocker):
    value = None

    @materialize(lazy=True, nout=5)
    def falsely_lazy_task():
        return (
            Table(pd.DataFrame({"x": [value]}), name="pd_table"),
            Table(pl.DataFrame({"x": [value]}), name="pl_table"),
            Table(pl.DataFrame({"x": [value]}).lazy(), name="pl_lazy_table"),
            select_as(2, "y"),
            3,
        )

    def get_flow():
        with Flow() as flow:
            with Stage("stage_1"):
                pd_tbl, pl_tbl, pl_lazy_tbl, select_tbl, constant = falsely_lazy_task()
                res_pd = m.take_first(pd_tbl, as_int=True)
                res_pl = m.take_first(pl_tbl, as_int=True)
                res_pl_lazy = m.take_first(pl_lazy_tbl, as_int=True)
                res_select = m.noop(select_tbl)
                res_constant = m.noop(constant)
        return flow, res_pd, res_pl, res_pl_lazy, res_select, res_constant

    value = 0
    flow, res_pd, res_pl, res_pl_lazy, res_select, res_constant = get_flow()
    with StageLockContext():
        result = flow.run()
        assert result.get(res_pd) == 0
        assert result.get(res_pl) == 0
        assert result.get(res_pl_lazy) == 0

    value = 1
    flow, res_pd, res_pl, res_pl_lazy, res_select, res_constant = get_flow()
    res_pd_spy = spy_task(mocker, res_pd)
    res_pl_spy = spy_task(mocker, res_pl)
    res_pl_lazy_spy = spy_task(mocker, res_pl_lazy)
    select_spy = spy_task(mocker, res_select)
    constant_spy = spy_task(mocker, res_constant)
    with StageLockContext():
        result = flow.run()
        assert result.get(res_pd) == 1
        assert result.get(res_pl) == 1
        assert result.get(res_pl_lazy) == 1
        # res_pd is downstream of a pd.DataFrame from a lazy task,
        # which should always be cache invalid. Hence, it should always be called.
        res_pd_spy.assert_called_once()

        # res_pd is downstream of a pl.DataFrame from a lazy task,
        # which should always be cache invalid. Hence, it should always be called.
        res_pl_spy.assert_called_once()

        # res_pd is downstream of a pl.LazyFrame from a lazy task,
        # which should always be cache invalid. Hence, it should always be called.
        # To avoid cache-invalidating the LazyFrame, we should use AUTOVERSION.
        res_pl_lazy_spy.assert_called_once()

        # res_select is downstream of an SQL query `select_tbl` (which did not change)
        # from a lazy task, hence select_tbl should be cache valid and the task
        # producing res_select should not be called.
        select_spy.assert_not_called()

        # res_constant is downstream of the constant `constant` (which did not change)
        # from a lazy task, hence res_constant should be cache valid and the task
        # producing res_select should not be called.
        constant_spy.assert_not_called()
