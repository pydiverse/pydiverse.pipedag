from __future__ import annotations

import pytest

from pydiverse.pipedag import Flow, Stage, Task, materialize
from pydiverse.pipedag.context import FinalTaskState
from pydiverse.pipedag.context.context import CacheValidationMode
from pydiverse.pipedag.errors import CacheError
from tests.fixtures.instances import with_instances
from tests.util import select_as
from tests.util import tasks_library as m
from tests.util.spy import spy_task


def test_run_specific_task(mocker):
    cache_function_call_allowed = True

    def assert_task_state(
        task_states: dict[Task, FinalTaskState],
        task_name: str,
        expected_state: FinalTaskState,
    ):
        tasks = [t for t in task_states if t.name == task_name]
        assert len(tasks) > 0
        assert all(task_states[t] == expected_state for t in tasks)

    def cache():
        assert (
            cache_function_call_allowed
        ), "Cache function call not allowed with disable_cache_function=True"
        return 0

    @materialize(lazy=True, cache=cache)
    def task_with_cache_fun():
        return select_as(0, "x")

    # We need to assign unique names to these stages, because we can't reuse the
    # same stage lock context between different runs.
    with Flow() as f:
        with Stage("subflow_t1") as s1:
            x1 = m.one()
            x2 = m.two()
            l1 = m.one_sql_lazy()
            task_with_cache_fun()

        with Stage("subflow_t2") as s2:
            y1 = m.create_tuple(x1, x2)
            y2 = m.noop(y1)

    f.run()

    x1_spy = spy_task(mocker, x1)
    x2_spy = spy_task(mocker, x2)
    l1_spy = spy_task(mocker, l1)
    s1_spy = spy_task(mocker, s1.commit_task)
    y1_spy = spy_task(mocker, y1)
    y2_spy = spy_task(mocker, y2)
    s2_spy = spy_task(mocker, s2.commit_task)

    # Run single task separately

    res = f.run(x1)
    x1_spy.assert_called_once()
    x2_spy.assert_not_called()
    l1_spy.assert_not_called()
    s1_spy.assert_not_called()
    y1_spy.assert_not_called()
    y2_spy.assert_not_called()
    s2_spy.assert_not_called()
    assert_task_state(res.task_states, x1.name, FinalTaskState.COMPLETED)

    res = f.run(y1)
    x1_spy.assert_not_called()
    x2_spy.assert_not_called()
    l1_spy.assert_not_called()
    s1_spy.assert_not_called()
    y1_spy.assert_called_once()
    y2_spy.assert_not_called()
    s2_spy.assert_not_called()
    assert_task_state(res.task_states, y1.name, FinalTaskState.COMPLETED)

    # Run multiple tasks at once

    res = f.run(x2, y2)
    x1_spy.assert_not_called()
    x2_spy.assert_called_once()
    l1_spy.assert_not_called()
    s1_spy.assert_not_called()
    y1_spy.assert_not_called()
    y2_spy.assert_called_once()
    s2_spy.assert_not_called()
    assert_task_state(res.task_states, x2.name, FinalTaskState.COMPLETED)
    assert_task_state(res.task_states, y2.name, FinalTaskState.COMPLETED)

    res = f.run(l1)
    x1_spy.assert_not_called()
    x2_spy.assert_not_called()
    l1_spy.assert_called_once()
    s1_spy.assert_not_called()
    y1_spy.assert_not_called()
    y2_spy.assert_not_called()
    s2_spy.assert_not_called()
    assert_task_state(res.task_states, l1.name, FinalTaskState.COMPLETED)

    cache_function_call_allowed = False

    res = f.run(
        cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID,
        disable_cache_function=True,
    )
    x1_spy.assert_called_once()
    x2_spy.assert_called_once()
    l1_spy.assert_called_once()
    s1_spy.assert_called_once()
    y1_spy.assert_called_once()
    y2_spy.assert_called_once()
    s2_spy.assert_called_once()
    assert_task_state(res.task_states, x1.name, FinalTaskState.COMPLETED)
    assert_task_state(res.task_states, x2.name, FinalTaskState.COMPLETED)
    assert_task_state(res.task_states, l1.name, FinalTaskState.COMPLETED)
    assert_task_state(res.task_states, y1.name, FinalTaskState.COMPLETED)
    assert_task_state(res.task_states, y2.name, FinalTaskState.COMPLETED)


def test_run_specific_task_ambiguous_input(mocker):
    with Flow() as f:
        with Stage("subflow_t1") as s1:
            # The same task appears multiple times in the schema. As a result, we must
            # use the tasks position hash to identify which instance of the noop task
            # we want to use as input for y1
            x1 = m.noop(1)
            x2 = m.noop(2)

        with Stage("subflow_t2") as s2:
            y1 = m.noop(x1)

    f.run()

    x1_spy = spy_task(mocker, x1)
    x2_spy = spy_task(mocker, x2)
    s1_spy = spy_task(mocker, s1.commit_task)
    y1_spy = spy_task(mocker, y1)
    s2_spy = spy_task(mocker, s2.commit_task)

    f.run(y1)

    x1_spy.assert_not_called()
    x2_spy.assert_not_called()
    s1_spy.assert_not_called()
    y1_spy.assert_called_once()
    s2_spy.assert_not_called()


def test_run_specific_stage(mocker):
    with Flow() as f:
        with Stage("subflow_s1") as s1:
            with Stage("subflow_s2") as s2:
                x1 = m.one()
                x2 = m.two()

            y1 = m.create_tuple(x1, x2)
            y2 = m.noop(y1)

        with Stage("subflow_s3") as s3:
            z1 = m.noop(x1)
            z2 = m.create_tuple(z1, y2)

    f.run()

    x1_spy = spy_task(mocker, x1)
    x2_spy = spy_task(mocker, x2)
    s1_spy = spy_task(mocker, s1.commit_task)
    y1_spy = spy_task(mocker, y1)
    y2_spy = spy_task(mocker, y2)
    s2_spy = spy_task(mocker, s2.commit_task)
    z1_spy = spy_task(mocker, z1)
    z2_spy = spy_task(mocker, z2)
    s3_spy = spy_task(mocker, s3.commit_task)

    # Run single stage

    f.run(s2)  # Inner stage

    x1_spy.assert_not_called()
    x2_spy.assert_not_called()
    s1_spy.assert_not_called()
    y1_spy.assert_not_called()  # Should be cached
    y2_spy.assert_not_called()  # Should be cached
    s2_spy.assert_called_once()
    z1_spy.assert_not_called()
    z2_spy.assert_not_called()
    s3_spy.assert_not_called()

    f.run(s1)  # Outer stage

    x1_spy.assert_not_called()  # Should be cached
    x2_spy.assert_not_called()  # Should be cached
    s1_spy.assert_called_once()
    y1_spy.assert_not_called()  # Should be cached
    y2_spy.assert_not_called()  # Should be cached
    s2_spy.assert_called_once()
    z1_spy.assert_not_called()
    z2_spy.assert_not_called()
    s3_spy.assert_not_called()

    # Run multiple stags

    f.run(s2, s3)

    x1_spy.assert_not_called()
    x2_spy.assert_not_called()
    s1_spy.assert_not_called()
    y1_spy.assert_not_called()  # Should be cached
    y2_spy.assert_not_called()  # Should be cached
    s2_spy.assert_called_once()
    z1_spy.assert_not_called()  # Should be cached
    z2_spy.assert_not_called()  # Should be cached
    s3_spy.assert_called_once()


def test_run_specific_stage_ambiguous_input(mocker):
    with Flow() as f:
        with Stage("subflow_s1") as s1:
            x1 = m.create_tuple(1, 1)
            x2 = m.create_tuple(x1, x1)

        with Stage("subflow_s2") as s2:
            y1 = m.create_tuple(x1, x2)

    f.run()

    x1_spy = spy_task(mocker, x1)
    x2_spy = spy_task(mocker, x2)
    s1_spy = spy_task(mocker, s1.commit_task)
    y1_spy = spy_task(mocker, y1)
    s2_spy = spy_task(mocker, s2.commit_task)

    # This test is non-trivial, because the task y1 has to fetch its inputs from
    # the cache. However, there are two `create_tuple` tasks in stage s1.
    # Consequently, it isn't clear which of the two tasks should be used.
    # To solve this, we use the position hash to identify which instance of a specific
    # task we are referring to.

    f.run(s2)

    x1_spy.assert_not_called()
    x2_spy.assert_not_called()
    s1_spy.assert_not_called()
    y1_spy.assert_not_called()  # Should be cached
    s2_spy.assert_called_once()


@with_instances("postgres")
def test_run_specific_task_with_table_blob():
    with Flow() as f:
        with Stage("subflow_t1"):
            x1 = m.noop({"x": [1]})
            x2 = m.noop({"x": [2]})
            x3 = m.as_blob("blob")
            x4 = m.noop(x3)

        with Stage("subflow_t2"):
            y1 = m.pd_dataframe(x1)
            y2 = m.pd_dataframe(x2)
            y3 = m.noop((y1, y2, x3, x4))

        with Stage("subflow_t3"):
            z1 = m.noop(y3)
            z2 = m.noop(z1)

    f.run()
    f.run(x4)
    f.run(y1)
    f.run(y3)
    f.run(x1, y2, z1)
    f.run(z2)


@with_instances("postgres")
def test_run_specific_stage_with_table_blob():
    with Flow() as f:
        with Stage("subflow_t1") as s1:
            x1 = m.noop({"x": [1]})
            x2 = m.noop({"x": [2]})
            x3 = m.as_blob("blob")
            x4 = m.noop(x3)

        with Stage("subflow_t2") as s2:
            y1 = m.pd_dataframe(x1)
            y2 = m.pd_dataframe(x2)
            y3 = m.noop((y1, y2, x3, x4))

        with Stage("subflow_t3") as s3:
            z1 = m.noop(y3)
            m.noop(z1)

    f.run(s1)
    f.run(s2)
    f.run(s3)
    f.run(s1, s3)


@with_instances("postgres")
def test_ignore_position_hashes():
    with Flow() as f:
        with Stage("subflow_s1"):
            x1 = m.create_tuple(1, 1)
            x2 = m.create_tuple(x1, x1)

        with Stage("subflow_s2") as s2:
            _ = m.create_tuple(x1, x2)
    f.run()

    # modify flow
    with Flow() as f:
        with Stage("subflow_s1"):
            x1 = m.create_tuple(1, 1)
            x2 = m.create_tuple(2, 2)

        with Stage("subflow_s2") as s2:
            _ = m.create_tuple(x1, x2)

    # as x2 was modified the subflow evaluation should raise a CacheError
    # because an x2 is not found with the correct position hash
    with pytest.raises(CacheError):
        f.run(s2)
    # ignoring the position hash of x2 should make this runnable
    f.run(s2, ignore_position_hashes=True)
