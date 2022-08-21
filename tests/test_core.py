from __future__ import annotations

import networkx as nx
import pytest

from pydiverse.pipedag import Flow, Stage
from pydiverse.pipedag.core import Task
from pydiverse.pipedag.errors import DuplicateNameError, FlowError, StageError


def t(name: str, **kwargs):
    return Task((lambda *a: a), name=f"task-{name}", **kwargs)


def validate_dependencies(flow: Flow):
    # Tasks outside a stage can only run if all
    # tasks inside the stage have finished.

    g = flow.graph
    expl_g = flow.build_graph()

    assert nx.is_directed_acyclic_graph(g)
    assert nx.is_directed_acyclic_graph(expl_g)

    stages = flow.stages.values()
    tasks = flow.tasks

    # Check flow.graph is correct
    for task in tasks:
        assert task in g
        assert task in expl_g

        parents = {edge[0] for edge in g.in_edges(task)}
        assert set(task.input_tasks.values()) == parents

    # Check inputs computed before task
    for task in tasks:
        for input_task in task.input_tasks.values():
            assert nx.shortest_path(expl_g, input_task, task)

    # Check each task in stage happens before commit
    for task in tasks:
        assert nx.shortest_path(expl_g, task, task.stage.commit_task)

    # Check commit task dependencies
    for child in tasks:
        for parent, _ in g.in_edges(child):  # type: Task
            if child.stage == parent.stage:
                continue

            if child.stage.is_inner(parent.stage):
                continue

            assert nx.shortest_path(expl_g, parent, parent.stage.commit_task)
            assert nx.shortest_path(expl_g, parent.stage.commit_task, child)

    # Ensure that nested stages get committed before their parents
    for stage in stages:
        if stage.outer_stage is None:
            continue
        assert nx.shortest_path(
            expl_g, stage.commit_task, stage.outer_stage.commit_task
        )


class TestDAGConstruction:
    """
    Test that the DAG gets constructed properly, all metadata is set
    properly and that all task dependencies are correct.
    """

    def test_one_task(self):
        with Flow("f") as f:
            with Stage("s") as s:
                t0 = t("0")(0)

        assert f.stages == {"s": s}
        assert f.tasks == [t0, s.commit_task]

        assert s.tasks == [t0]
        assert s.outer_stage is None

        assert t0.upstream_stages == []

        validate_dependencies(f)

    def test_two_independent_tasks(self):
        with Flow("f") as f:
            with Stage("s") as s:
                t0 = t("0")(0)
                t1 = t("1")(1)

        assert f.stages == {"s": s}
        assert f.tasks == [t0, t1, s.commit_task]

        assert s.tasks == [t0, t1]
        assert s.outer_stage is None

        assert t0.upstream_stages == []
        assert t1.upstream_stages == []

        validate_dependencies(f)

    def test_two_connected_tasks(self):
        with Flow("f") as f:
            with Stage("s") as s:
                t0 = t("0")(0)
                t1 = t("1")(t0)

        assert f.stages == {"s": s}
        assert f.tasks == [t0, t1, s.commit_task]

        assert s.tasks == [t0, t1]
        assert s.outer_stage is None

        assert t0.upstream_stages == []
        assert t1.upstream_stages == [s]

        validate_dependencies(f)

    def test_two_stages(self):
        with Flow("f") as f:
            with Stage("stage 0") as s0:
                t00 = t("00")(0)
                t01 = t("01")(t00)

            with Stage("stage 1") as s1:
                t10 = t("10")(t00)
                t11 = t("11")(t10, t00)
                t12 = t("12")(t00)

        assert f.stages == {"stage 0": s0, "stage 1": s1}
        assert f.tasks == [t00, t01, s0.commit_task, t10, t11, t12, s1.commit_task]

        assert s0.tasks == [t00, t01]
        assert s0.outer_stage is None

        assert s1.tasks == [t10, t11, t12]
        assert s1.outer_stage is None

        assert t00.upstream_stages == []
        assert t01.upstream_stages == [s0]
        assert t10.upstream_stages == [s0]
        assert t11.upstream_stages == [s1, s0]
        assert t12.upstream_stages == [s0]

        validate_dependencies(f)

    def test_nested(self):
        with Flow("f") as f:
            with Stage("stage 0") as s0:
                t00 = t("00")(0)
                t01 = t("01")(t00)

            with Stage("stage 1") as s1:
                t10 = t("10")(t00)
                t11 = t("11")(t10, t00)
                t12 = t("12")(t00)

                with Stage("stage 2") as s2:
                    t20 = t("20")(t11)

            with Stage("stage 3") as s3:
                t30 = t("30")(t12, t20)
                t31 = t("31")(t00, t10, t20, t30)

        assert f.stages == {"stage 0": s0, "stage 1": s1, "stage 2": s2, "stage 3": s3}
        assert f.tasks == [
            t00,
            t01,
            s0.commit_task,
            t10,
            t11,
            t12,
            t20,
            s2.commit_task,
            s1.commit_task,
            t30,
            t31,
            s3.commit_task,
        ]

        assert s0.tasks == [t00, t01]
        assert s0.outer_stage is None

        assert s1.tasks == [t10, t11, t12]
        assert s1.outer_stage is None

        assert s2.tasks == [t20]
        assert s2.outer_stage is s1
        assert s2.is_inner(s1)

        assert s3.tasks == [t30, t31]
        assert s3.outer_stage is None

        assert t00.upstream_stages == []
        assert t01.upstream_stages == [s0]
        assert t10.upstream_stages == [s0]
        assert t11.upstream_stages == [s1, s0]
        assert t12.upstream_stages == [s0]
        assert t20.upstream_stages == [s1]
        assert t30.upstream_stages == [s1, s2]
        assert t31.upstream_stages == [s0, s1, s2, s3]

        validate_dependencies(f)

    def test_ids(self):
        with Flow("f"):
            with Stage("stage 0") as s0:
                t00 = t("00")(0)
                t01 = t("01")(t00)

            with Stage("stage 1") as s1:
                t10 = t("10")(t00)
                t11 = t("11")(t10, t00)
                t12 = t("12")(t00)

                with Stage("stage 2") as s2:
                    t20 = t("20")(t11)

            with Stage("stage 3") as s3:
                t30 = t("30")(t12, t20)
                t31 = t("31")(t00, t10, t20, t30)

        assert [stage.id for stage in [s0, s1, s2, s3]] == [0, 1, 2, 3]
        assert [
            task.id
            for task in [
                t00,
                t01,
                s0.commit_task,
                t10,
                t11,
                t12,
                t20,
                s2.commit_task,
                s1.commit_task,
                t30,
                t31,
                s3.commit_task,
            ]
        ] == list(range(12))


class TestDAGConstructionExceptions:
    def test_duplicate_stage_name(self):
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

            # Different capitalization
            with pytest.raises(DuplicateNameError):
                with Stage("Stage"):
                    ...

        # Should be able to reuse name in different flow
        with Flow("flow 2"):
            with Stage("stage"):
                ...

    def test_reuse_stage(self):
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

    def test_stage_outside_flow(self):
        with pytest.raises(StageError):
            with Stage("stage"):
                ...

    def test_task_outside_flow(self):
        with pytest.raises(FlowError):
            t("task")()

    def test_task_outside_stage(self):
        with Flow("flow"):
            with pytest.raises(StageError):
                t("task")()

    def test_task_in_wrong_flow(self):
        with Flow("flow 1"):
            with Stage("stage"):
                task = t("task")()

        with Flow("flow 2"):
            with Stage("stage"):
                with pytest.raises(FlowError):
                    # Can't use task from different flow as argument
                    t("bad task")(task)


def test_task_nout():
    with Flow("flow"):
        with Stage("stage"):
            _ = t("task")
            _ = t("task", nout=1)

            _, _ = t("task", nout=2)
            _, _, _ = t("task", nout=3)
            _, _, *_ = t("task", nout=10)

            with pytest.raises(ValueError):
                _, _ = t("task")

            with pytest.raises(ValueError):
                t("task", nout=0)
            with pytest.raises(ValueError):
                t("task", nout=1.5)


def test_task_getitem():
    with Flow("flow"):
        with Stage("stage"):
            task = t("task")()

            _ = task[0]
            _ = task[1]
            _ = task[0][0]
            _ = task["something"][0:8]
