from __future__ import annotations

import datetime
import time

import pytest
from pytest_mock import MockerFixture

from pydiverse.pipedag import Flow, Stage, materialise

# noinspection PyUnresolvedReferences
from tests.util import setup_pipedag


@materialise()
def m_1():
    return 1


@materialise()
def m_2():
    return 2


@materialise(nout=2)
def m_tuple(a, b):
    return a, b


@materialise()
def m_noop(x):
    return x


@materialise()
def m_sleep_noop(x, duration=0.5):
    time.sleep(duration)
    return x


@materialise()
def m_raise(x, r: bool):
    time.sleep(0.1)
    if r:
        raise Exception
    return x


def m_assert(condition):
    @materialise(lazy=True)
    def _m_assert(x):
        assert condition(x)
        return x

    return _m_assert


def test_stage_attach_tasks():
    with Flow("flow"):
        with Stage("stage") as s:
            task_1 = m_1()
            task_2 = m_2()

    assert s.tasks == [task_1, task_2]
    assert task_1.stage is s
    assert task_2.stage is s
    assert task_1.upstream_stages == []
    assert task_2.upstream_stages == []


def test_nested_stage_attach_tasks():
    with Flow("flow"):
        with Stage("outer") as outer:
            task_1_outer = m_1()
            with Stage("inner") as inner:
                task_1_inner = m_1()
                task_2_inner = m_2()
            task_2_outer = m_2()

    assert outer.tasks == [task_1_outer, task_2_outer]
    assert task_1_outer.stage is outer
    assert task_2_outer.stage is outer
    assert task_1_inner.upstream_stages == []
    assert task_2_inner.upstream_stages == []

    assert inner.tasks == [task_1_inner, task_2_inner]
    assert task_1_inner.stage is inner
    assert task_2_inner.stage is inner
    assert task_1_inner.upstream_stages == []
    assert task_2_inner.upstream_stages == []


def test_task_upstream_stages():
    with Flow("flow"):
        with Stage("stage1") as s1:
            task_1 = m_1()
            task_1_list = m_noop.map([task_1])

        with Stage("stage2") as s2:
            task_2 = m_2()
            task_2_list = m_noop.map([task_2])

        with Stage("stage3") as s3:
            task_tuple = m_tuple(task_1_list, task_2_list)
            task_tuple_map = m_noop.map(task_tuple)
            task_list_map = m_noop.map([task_1, task_tuple_map])

    assert task_1.upstream_stages == []
    assert task_1_list.upstream_stages == []
    assert task_2.upstream_stages == []
    assert task_2_list.upstream_stages == []
    assert set(task_tuple.upstream_stages) == {s1, s2}
    assert task_tuple_map.upstream_stages == []
    assert task_list_map.upstream_stages == [s1]


def test_stage_ref_counter(mocker: MockerFixture):
    with Flow("flow"):
        with Stage("stage") as s:
            task_1 = m_1()
            task_2 = m_2()

    stub = mocker.stub("stage ref counter free")
    s._set_ref_count_free_handler(stub)

    assert s._ref_count == 3
    task_1._decr_stage_ref_count()
    task_2._decr_stage_ref_count()
    assert s._ref_count == 1

    stub.assert_not_called()
    s.task._decr_stage_ref_count()
    assert s._ref_count == 0
    stub.assert_called_once_with(s)


def test_stage_ref_counter_auto():
    # Super simple case with just two task inside one stage
    with Flow("flow") as f:
        with Stage("stage") as s:
            task_1 = m_1()
            task_2 = m_2()
            m_assert(lambda _: s._ref_count == 2)([task_1, task_2])

    assert f.run().is_successful()
    assert s._ref_count == 0

    # Multiple tasks with interdependency and a map inside one stage
    with Flow("flow") as f:
        with Stage("stage2") as s:
            task_1 = m_1()
            task_2 = m_2()
            task_tuple = m_tuple(task_1, task_2)
            task_tuple = m_assert(lambda _: s._ref_count == 4)(task_tuple)
            task_map = m_noop.map(task_tuple)
            m_assert(lambda _: s._ref_count == 2)(task_map)

    assert f.run().is_successful()
    assert s._ref_count == 0

    # Multiple tasks spread over multiple stages
    with Flow("flow") as f:
        with Stage("stage3") as s1:
            task_1 = m_1()
            task_1_list = m_noop.map([task_1])
            m_assert(lambda _: s1._ref_count == 3)(task_1_list)

        with Stage("stage4") as s2:
            task_2 = m_2()
            task_2_list = m_noop.map([task_2])
            m_assert(lambda _: s2._ref_count == 3)(task_2_list)

        with Stage("stage5") as s3:
            task_tuple = m_tuple(task_1_list, task_2_list)
            x = m_assert(lambda _: s1._ref_count == 0)(task_tuple)
            x = m_assert(lambda _: s2._ref_count == 0)(x)
            m_assert(lambda _: s3._ref_count == 2)(x)

    assert f.run().is_successful()


def test_materialise_memo():
    # A flow should be able to contain the same task with the same inputs
    # more than once and still run successfully.
    with Flow("flow") as f:
        with Stage("stage1"):
            t_1 = m_sleep_noop(1)
            t_2 = m_sleep_noop(1)
            t_3 = m_sleep_noop(1)
            t_4 = m_sleep_noop(1)

        with Stage("stage2"):
            t_5 = m_sleep_noop(1)
            t_6 = m_sleep_noop(t_5)
            t_7 = m_sleep_noop(t_6)
            t_8 = m_sleep_noop(t_7)

        with Stage("stage3"):
            t_map = m_noop([t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8])

    assert f.run().is_successful()


@pytest.mark.timeout(5)
def test_materialise_memo_with_failures():
    with Flow("flow") as f:
        with Stage("stage1"):
            t_1 = m_raise(1, False)
            t_2 = m_raise(1, True)
            t_3 = m_raise(1, True)
            t_4 = m_raise(t_2, True)

        with Stage("stage2"):
            t_5 = m_raise_retry(1, False)
            t_6 = m_raise_retry(t_5, False)
            t_7 = m_raise_retry(t_6, True)
            t_8 = m_raise_retry(1, True)

        with Stage("stage3"):
            t_map = m_noop([t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8])

    assert f.run().is_failed()
