from __future__ import annotations

import random
import time
import uuid

import pytest
import sqlalchemy as sa

from pydiverse.pipedag import Flow, GroupNode, Stage, VisualizationStyle, materialize
from pydiverse.pipedag.context.context import (
    ConfigContext,
    StageLockContext,
    TaskContext,
)
from pydiverse.pipedag.util.hashing import stable_hash
from tests.fixtures.instances import (
    ORCHESTRATION_INSTANCES,
    skip_instances,
    with_instances,
)
from tests.util import tasks_library as m

pytestmark = [with_instances("postgres", ORCHESTRATION_INSTANCES)]


@pytest.mark.parametrize("ordering_barrier", [True, False])
def test_run_specific_task(ordering_barrier):
    store = ConfigContext.get().store.table_store
    table_name = stable_hash(ordering_barrier, store.engine.url)

    def cache():
        # ensure different position hash for each task
        return uuid.uuid4().hex

    @materialize(cache=cache)
    def task():
        time.sleep(random.randint(0, 1) * 0.1)
        engine = ConfigContext.get().store.table_store.engine
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    sa.text(f'LOCK TABLE "{table_name}" IN ACCESS EXCLUSIVE MODE')
                )
                conn.execute(sa.text(f'INSERT INTO "{table_name}" VALUES (1)'))
                n = conn.execute(
                    sa.text(f'SELECT COUNT(*) FROM "{table_name}"')
                ).fetchone()[0]
        TaskContext.get().task.logger.info(f"Task result:{n}")
        return n

    # We need to assign unique names to these stages, because we can't reuse the
    # same stage lock context between different runs.
    with Flow() as f:
        with Stage("stage_1"):
            # smoke test
            with GroupNode(ordering_barrier=ordering_barrier):
                _ = m.noop(1)
            _ = m.noop(2)

        with Stage("stage_2"):
            # test barrier ordering
            x1 = task()
            x2 = task()
            with GroupNode(ordering_barrier=ordering_barrier):
                x3 = task()
                x4 = task()
            x5 = task()

        with Stage("stage_3"):
            # smoke test
            _ = m.noop(3)
            with GroupNode(ordering_barrier=ordering_barrier):
                _ = m.noop(4)

        with Stage("stage_4"):
            # smoke test
            with GroupNode(ordering_barrier=ordering_barrier):
                _ = m.noop(5)

    barriers = 0
    if ordering_barrier:
        barriers = 1 + 2 + 2 + 1
    assert len(f.tasks) == 2 + 5 + 2 + 1 + len(f.stages) + barriers

    store.execute(f'DROP TABLE IF EXISTS "{table_name}" ')
    store.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" (x INT)')

    with StageLockContext():
        res = f.run()
        assert sum(res.get(x) for x in [x1, x2, x3, x4, x5]) == sum(range(1, 6))
        if ordering_barrier:
            assert res.get(x1) < res.get(x3)
            assert res.get(x2) < res.get(x3)
            assert res.get(x1) < res.get(x4)
            assert res.get(x2) < res.get(x4)
            assert res.get(x3) < res.get(x5)
            assert res.get(x4) < res.get(x5)

    with StageLockContext():
        res = f.run(x1, x3)
        assert sum(res.get(x) for x in [x1, x3]) == sum(range(6, 8))
        if ordering_barrier:
            assert res.get(x1) < res.get(x3)

    store.execute(f'DROP TABLE "{table_name}"')


@with_instances("postgres")
@skip_instances(ORCHESTRATION_INSTANCES)
@pytest.mark.parametrize("label", ["group"])
@pytest.mark.parametrize(
    "style",
    [
        VisualizationStyle(),
        VisualizationStyle(hide_label=True),
        VisualizationStyle(hide_content=True, hide_label=True),
        VisualizationStyle(box_color_always="aaaa22"),
    ],
)
def test_run_specific_task_sequential_styles(label, style):
    test_run_specific_task_sequential(
        label, style, ordering_barrier=False, nesting=True
    )


@with_instances("postgres")
@skip_instances(ORCHESTRATION_INSTANCES)
@pytest.mark.parametrize("label", [None, "group"])
@pytest.mark.parametrize(
    "style",
    [
        None,
        VisualizationStyle(hide_box=True),
        VisualizationStyle(hide_box=True, hide_content=True),
        VisualizationStyle(hide_content=True),
        VisualizationStyle(),
    ],
)
@pytest.mark.parametrize("ordering_barrier", [True, False])
@pytest.mark.parametrize("nesting", [False])
def test_run_specific_task_sequential(label, style, ordering_barrier, nesting):
    num = [0]

    def cache():
        return num[0]

    @materialize(cache=cache)
    def task():
        time.sleep(random.randint(0, 1) * 0.1)
        # random parameter is needed to make sure task is called multiple times
        # despite identical position hash
        num[0] += 1
        return num[0]

    # We need to assign unique names to these stages, because we can't reuse the
    # same stage lock context between different runs.
    with Flow() as f:
        with GroupNode(label, style, ordering_barrier=ordering_barrier):
            with Stage("stage_1"):
                one = m.noop(1)
        with Stage("stage_2"):
            _ = m.noop(one)
            x1 = task()
            x2 = task()
            with GroupNode(label, style, ordering_barrier=ordering_barrier):
                x3 = task()
                x4 = task()
            x5 = task()
        if nesting:
            with GroupNode(label, style, ordering_barrier=ordering_barrier):
                with Stage("stage_3"):
                    with Stage("stage_4"):
                        with GroupNode(label, style, ordering_barrier=ordering_barrier):
                            with GroupNode(
                                label, style, ordering_barrier=ordering_barrier
                            ):
                                _ = m.noop(1)
                            _ = m.noop(2)
                        _ = m.noop(3)
                    _ = m.noop(4)
                with Stage("stage_5"):
                    _ = m.noop(5)
            with GroupNode(label, style, ordering_barrier=ordering_barrier):
                with Stage("stage_6"):
                    with Stage("stage_7"):
                        with GroupNode(label, style, ordering_barrier=ordering_barrier):
                            with GroupNode(
                                label, style, ordering_barrier=ordering_barrier
                            ):
                                _ = m.noop(6)

    if not nesting:
        barriers = 0
        if ordering_barrier:
            barriers = 2
        assert len(f.tasks) == 2 + 5 + len(f.stages) + barriers

    with StageLockContext():
        res = f.run()
        assert res.get(x1) == 1
        assert res.get(x2) == 2
        assert res.get(x3) == 3
        assert res.get(x4) == 4
        assert res.get(x5) == 5

    with StageLockContext():
        res = f.run(x1, x3)
        assert res.get(x1) == 6
        assert res.get(x3) == 7
