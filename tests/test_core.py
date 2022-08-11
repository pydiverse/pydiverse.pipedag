from __future__ import annotations

import time

import pytest
from pytest_mock import MockerFixture

from pydiverse.pipedag import Flow, Stage, materialise
from pydiverse.pipedag.errors import DuplicateNameError, FlowError, StageError

# noinspection PyUnresolvedReferences
from tests.util import setup_pipedag


@materialise
def m_1():
    return 1


@materialise
def m_2():
    return 2


@materialise(nout=2)
def m_tuple(a, b):
    return a, b


@materialise
def m_noop(x):
    return x


@materialise
def m_sleep_noop(x, duration=0.05):
    time.sleep(duration)
    return x


@materialise
def m_raise(x, r: bool):
    time.sleep(0.05)
    if r:
        raise Exception
    return x


def m_assert(condition):
    @materialise(lazy=True)
    def _m_assert(x):
        assert condition(x)
        return x

    return _m_assert


def test_task_attach_to_stage():
    with Flow("flow"):
        with Stage("stage") as s:
            task_1 = m_1()
            task_2 = m_2()

    assert s.tasks == [task_1, task_2]
    assert task_1.stage is s
    assert task_2.stage is s
    assert task_1.upstream_stages == []
    assert task_2.upstream_stages == []


def test_task_attach_to_nested_stage():
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

    assert inner.outer_stage is outer
    assert inner.is_inner(outer)


def test_stage_ref_counter():
    # Super simple case with just two task inside one stage
    with Flow("flow") as f:
        with Stage("stage") as s:
            task_1 = m_1()
            task_2 = m_2()

            # One reference from the m_assert and commit
            m_assert(lambda _: s.ref_count == 2)([task_1, task_2])

    assert f.run().is_successful()
    assert s.ref_count == 0

    # Multiple tasks with interdependency inside one stage
    with Flow("flow") as f:
        with Stage("stage") as s:
            task_1 = m_1()
            task_2 = m_2()
            task_tuple = m_tuple(task_1, task_2)
            # One reference from assert, noop, assert and commit
            task_tuple = m_assert(lambda _: s.ref_count == 4)(task_tuple)
            task_tuple = m_noop(task_tuple)
            # One reference from assert and commit
            m_assert(lambda _: s.ref_count == 2)(task_tuple)

    assert f.run().is_successful()
    assert s.ref_count == 0

    # Multiple tasks spread over multiple stages
    with Flow("flow") as f:
        with Stage("stage 1") as s1:
            task_1 = m_1()
            # One reference from assert, noop, assert, commit and downstream
            task_1 = m_assert(lambda _: s1.ref_count == 5)(task_1)
            task_1 = m_noop(task_1)
            # One reference from assert, commit and downstream
            m_assert(lambda _: s1.ref_count == 3)(task_1)

        with Stage("stage 2") as s2:
            task_2 = m_2()
            task_2 = m_noop([task_2])
            m_assert(lambda _: s2.ref_count == 3)(task_2)

        with Stage("stage 3") as s3:
            task_tuple = m_tuple(task_1, task_2)
            # Check that s1 and s2 have been freed
            x = m_assert(lambda _: s1.ref_count == 0)(task_tuple)
            x = m_assert(lambda _: s2.ref_count == 0)(x)
            m_assert(lambda _: s3.ref_count == 2)(x)

    assert s1.ref_count == 0
    assert s2.ref_count == 0
    assert s3.ref_count == 0
    assert f.run().is_successful()


def test_stage_ref_count_free_handler(mocker: MockerFixture):
    with Flow("flow") as f:
        with Stage("stage") as s:  # Commit task: 1 reference to s
            task_1 = m_1()  # No reference to s
            task_2 = m_2()  # No reference to s
            task_3 = m_noop([task_1, task_2])  # 1 reference to s

    stub = mocker.stub("stage ref counter free")
    s.set_ref_count_free_handler(stub)

    f.prepare_for_run()

    assert s.ref_count == 2
    task_1.on_success()
    task_2.on_failure()
    assert s.ref_count == 2
    task_3.on_success()
    assert s.ref_count == 1

    stub.assert_not_called()
    s.commit_task.on_success()
    assert s.ref_count == 0
    stub.assert_called_once_with(s)


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


def test_materialise_memo_with_failures():
    with Flow("flow") as f:
        with Stage("stage1"):
            t_1 = m_raise(1, False)
            t_2 = m_raise(1, True)
            t_3 = m_raise(1, True)
            t_4 = m_raise(t_2, True)

        with Stage("stage2"):
            t_5 = m_raise(1, False)
            t_6 = m_raise(t_5, False)
            t_7 = m_raise(t_6, True)
            t_8 = m_raise(1, True)

        with Stage("stage3"):
            t_map = m_noop([t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8])

    assert f.run().is_failed()


def test_duplicate_stage_name():
    with Flow("flow 1"):
        with Stage("stage"):
            # Nested
            with pytest.raises(DuplicateNameError):
                with Stage("stage"):
                    ...

        # Consecutive
        with pytest.raises(DuplicateNameError):
            with Stage("stage"):
                ...

    # Should be able to reuse name in different flow
    with Flow("flow 2"):
        with Stage("stage"):
            ...


def test_reuse_stage():
    with Flow("flow 1"):
        with Stage("stage") as s:
            # Nested
            with pytest.raises(StageError):
                with s:
                    ...

        # Consecutive
        with pytest.raises(StageError):
            with s:
                ...

    with Flow("flow 2"):
        # Different flow
        with pytest.raises(StageError):
            with s:
                ...


def test_task_outside_flow():
    with pytest.raises(FlowError):
        task = m_1()


def test_task_outside_stage():
    with Flow("flow"):
        with pytest.raises(StageError):
            task = m_1()


def test_reference_task_in_wrong_flow():
    with Flow("flow 1"):
        with Stage("stage"):
            task = m_1()

    with Flow("flow 2"):
        with Stage("stage"):
            with pytest.raises(FlowError):
                bad_task = m_noop(task)
