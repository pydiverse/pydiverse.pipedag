import pytest

from pydiverse.pipedag import Flow, Stage
from tests.util import tasks_library as m
from tests.util.spy import spy_task


def test_run_specific_task(mocker):
    # We need to assign unique names to these stages, because we can't reuse the
    # same stage lock context between different runs.
    with Flow() as f:
        with Stage("subflow_t1") as s1:
            x1 = m.one()
            x2 = m.two()

        with Stage("subflow_t2") as s2:
            y1 = m.create_tuple(x1, x2)
            y2 = m.noop(y1)

    f.run()

    x1_spy = spy_task(mocker, x1)
    x2_spy = spy_task(mocker, x2)
    s1_spy = spy_task(mocker, s1.commit_task)
    y1_spy = spy_task(mocker, y1)
    y2_spy = spy_task(mocker, y2)
    s2_spy = spy_task(mocker, s2.commit_task)

    # Run single task separately

    f.run(x1)
    x1_spy.assert_called_once()
    x2_spy.assert_not_called()
    s1_spy.assert_not_called()
    y1_spy.assert_not_called()
    y2_spy.assert_not_called()
    s2_spy.assert_not_called()

    f.run(y1)
    x1_spy.assert_not_called()
    x2_spy.assert_not_called()
    s1_spy.assert_not_called()
    y1_spy.assert_called_once()
    y2_spy.assert_not_called()
    s2_spy.assert_not_called()

    # Run multiple tasks at once

    f.run(x2, y2)
    x1_spy.assert_not_called()
    x2_spy.assert_called_once()
    s1_spy.assert_not_called()
    y1_spy.assert_not_called()
    y2_spy.assert_called_once()
    s2_spy.assert_not_called()


@pytest.mark.xfail(readon="Ambiguous input", strict=True)
def test_run_specific_task_ambiguous_input(mocker):
    with Flow() as f:
        with Stage("subflow_t1") as s1:
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

    # This is expected to fail because the task y1 has the task x1 as input. However,
    # because we need to fetch inputs for y1 from cache, we can only do so by
    # referencing the names and stages of the input tasks.
    # But in stage s1 there are two `noop` tasks, thus it isn't clear which one
    # to use as input for y1.

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


@pytest.mark.xfail(reason="Ambiguous input", strict=True)
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

    # Run multiple stages
    # This test is non-trivial, because the task y1 has to fetch its inputs from
    # the cache. However, there are two `create_tuple` tasks in stage s1.
    # Consequently, it isn't clear which of the two tasks should be used.

    f.run(s2)

    x1_spy.assert_not_called()
    x2_spy.assert_not_called()
    s1_spy.assert_not_called()
    y1_spy.assert_not_called()  # Should be cached
    s2_spy.assert_called_once()
