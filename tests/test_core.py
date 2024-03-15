from __future__ import annotations

import networkx as nx
import pytest

from pydiverse.pipedag import Flow, Stage
from pydiverse.pipedag.core import UnboundTask
from pydiverse.pipedag.errors import DuplicateNameError, FlowError, StageError


def t(name: str, **kwargs):
    return UnboundTask((lambda *a: a), name=f"task-{name}", **kwargs)


def t_kw(name: str, **kwargs):
    return UnboundTask((lambda **kw: kw), name=f"task-kw-{name}", **kwargs)


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
            with Stage("stage_0") as s0:
                t00 = t("00")(0)
                t01 = t("01")(t00)

            with Stage("stage_1") as s1:
                t10 = t("10")(t00)
                t11 = t("11")(t10, t00)
                t12 = t("12")(t00)

        assert f.stages == {"stage_0": s0, "stage_1": s1}
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
            with Stage("stage_0") as s0:
                t00 = t("00")(0)
                t01 = t("01")(t00)

            with Stage("stage_1") as s1:
                t10 = t("10")(t00)
                t11 = t("11")(t10, t00)
                t12 = t("12")(t00)

                with Stage("stage_2") as s2:
                    t20 = t("20")(t11)

            with Stage("stage_3") as s3:
                t30 = t("30")(t12, t20)
                t31 = t("31")(t00, t10, t20, t30)

        assert f.stages == {"stage_0": s0, "stage_1": s1, "stage_2": s2, "stage_3": s3}
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
            with Stage("stage_0") as s0:
                t00 = t("00")(0)
                t01 = t("01")(t00)

            with Stage("stage_1") as s1:
                t10 = t("10")(t00)
                t11 = t("11")(t10, t00)
                t12 = t("12")(t00)

                with Stage("stage_2") as s2:
                    t20 = t("20")(t11)

            with Stage("stage_3") as s3:
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

    def test_subflow(self):
        with Flow("f") as f:
            with Stage("stage_0"):
                t00 = t("00")(0)
                t01 = t("01")(t00)
                t02 = t("02")(t01)

            with Stage("stage_1") as s1:
                t10 = t("10")(t02)
                with Stage("stage_2") as s2:
                    t20 = t("20")(t10)
                t11 = t("11")(t20)
                t12 = t("12")(t10, t20, t11)

        # Entire Flow as Subflow
        sf = f.get_subflow()
        assert not sf.is_tasks_subflow
        assert set(sf.get_tasks()) == set(f.tasks)

        # Stage Subflow
        sf = f.get_subflow(s2)
        assert not sf.is_tasks_subflow
        assert set(sf.get_tasks()) == {t20, s2.commit_task}
        assert set(sf.get_parent_tasks(t20)) == set()

        # Stage Subflow
        sf = f.get_subflow(s1)
        assert not sf.is_tasks_subflow
        assert set(sf.get_tasks()) == {
            t10,
            t11,
            t12,
            t20,
            s1.commit_task,
            s2.commit_task,
        }
        assert set(sf.get_parent_tasks(t10)) == set()
        assert set(sf.get_parent_tasks(t11)) == {t20, s2.commit_task}

        # Task Subflow
        sf = f.get_subflow(t10, t12)
        assert sf.is_tasks_subflow
        assert set(sf.get_tasks()) == {t10, t12}
        assert set(sf.get_parent_tasks(t10)) == set()
        assert set(sf.get_parent_tasks(t12)) == {t10}


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


class TestPositionHash:
    def test_single_task(self):
        with Flow():
            with Stage("stage_0"):
                x = t("0")(0)
                y = t("0")(0)

                z0 = t("0")(1)  # Different input
                z1 = t("1")(0)  # Different name

            with Stage("stage_1"):
                z2 = t("0")(0)  # Different stage

        assert x.position_hash == y.position_hash

        assert x.position_hash != z0.position_hash
        assert x.position_hash != z1.position_hash
        assert x.position_hash != z2.position_hash

    def test_multiple_tasks(self):
        with Flow():
            with Stage("stage_0"):
                x0 = t("0")(0)
                y0 = t("0")(0)

                z0_0 = t("1")(0)  # Wrong task name

            with Stage("stage_1"):
                x1 = t("1")(2, x0)
                y1 = t("1")(2, y0)

                z1_0 = t("1")(2, z0_0)  # z0_0 has different position hash
                z1_1 = t("1")(1, x1)  # Different input: 1 != 2

            with Stage("stage_2"):
                x2 = t_kw("2")(a=1, b=2, c=[x1, y1])
                y2 = t_kw("2")(b=2, a=1, c=[y1, x1])

                z2_0 = t_kw("2")(A=1, b=2, c=[x1, y1])  # Different kwarg name
                z2_1 = t_kw("2")(a=1, b=2, c=[z1_0, z1_1])  # Inputs different pos hash

        assert x0.position_hash == y0.position_hash
        assert x0.position_hash != z0_0.position_hash

        assert x1.position_hash == y1.position_hash
        assert x1.position_hash != z1_0.position_hash
        assert x1.position_hash != z1_1.position_hash

        assert x2.position_hash == y2.position_hash
        assert x2.position_hash != z2_0.position_hash
        assert x2.position_hash != z2_1.position_hash

    def test_get_item(self):
        with Flow():
            with Stage("stage_0"):
                inputs = t_kw("inputs")(a=1, b=2)

                x0 = t("0")(inputs["a"])
                y0 = t("0")(inputs["a"])
                z0 = t("0")(inputs["b"])

            with Stage("stage_1"):
                inputs = t("inputs")(1, 2)

                x1 = t("0")(inputs[0])
                y1 = t("0")(inputs[0])
                z1 = t("0")(inputs[1])

        assert x0.position_hash == y0.position_hash
        assert x0.position_hash != z0.position_hash

        assert x1.position_hash == y1.position_hash
        assert x1.position_hash != z1.position_hash

        assert inputs["1"].position_hash != inputs[1].position_hash
        assert inputs[1].position_hash != inputs[1.0].position_hash
        assert inputs[1][1].position_hash != inputs[1][0].position_hash
        assert inputs[1][1].position_hash != inputs[0][1].position_hash


class TestFlow:
    def test_get_stage(self):
        with Flow() as f:
            with Stage("s0") as s0:
                ...

            with Stage("s1") as s1:
                ...

        assert f.get_stage("s0") == f["s0"] == s0
        assert f.get_stage("s1") == f["s1"] == s1

        with pytest.raises(KeyError):
            f.get_stage("s2")


class TestStage:
    def test_get_task(self):
        with Flow():
            with Stage("s0") as s0:
                t00_0 = t("00")(0)
                t00_1 = t("00")(1)

            with Stage("s1") as s1:
                t10 = t("10")(t00_0)
                t11 = t("11")(t00_1)
                t00_s1 = t("00")(0)

        assert s0.get_task(t00_0.name, 0) == s0[t00_0.name, 0] == t00_0
        assert s0.get_task(t00_1.name, 1) == s0[t00_0.name, 1] == t00_1

        assert s1.get_task(t10.name) == s1[t10.name] == t10
        assert s1.get_task(t10.name, 0) == s1[t10.name, 0] == t10
        assert s1.get_task(t11.name) == t11
        assert s1.get_task(t11.name, 0) == t11
        assert s1.get_task(t00_s1.name) == t00_s1
        assert s1.get_task(t00_s1.name, 0) == t00_s1

        with pytest.raises(LookupError):
            # Task doesn't exist
            s0.get_task("foo")
        with pytest.raises(ValueError):
            # Missing index
            s0.get_task(t00_0.name)
        with pytest.raises(IndexError):
            # Out of bounds
            s0.get_task(t00_0.name, 2)


def test_task_nout():
    with Flow("flow"):
        with Stage("stage"):
            _ = t("task")()
            _ = t("task", nout=1)()

            _, _ = t("task", nout=2)()
            _, _, _ = t("task", nout=3)()
            _, _, *_ = t("task", nout=10)()

            with pytest.raises(ValueError):
                _, _ = t("task")()

            with pytest.raises(ValueError):
                t("task", nout=0)
            with pytest.raises(ValueError):
                t("task", nout=1.5)


def test_task_getitem():
    with Flow("flow"):
        with Stage("stage"):
            task = t("task")()

    assert task.resolve_value((1, 2)) == (1, 2)
    assert task[0].resolve_value((1, 2)) == 1
    assert task[1].resolve_value((1, 2)) == 2
    assert task[1][0].resolve_value(((1, 2), (3, 4))) == 3
    assert task["x"][1:3].resolve_value({"x": [1, 2, 3, 4]}) == [2, 3]


def test_task_outside_flow():
    task = t("task")
    assert task(1, 2) == (1, 2)
    assert task("foo", "bar") == ("foo", "bar")

    task = t_kw("task")
    assert task(a=1, b=2) == {"a": 1, "b": 2}
    assert task(x="foo", y="bar") == {"x": "foo", "y": "bar"}


def test_flow_visualize_url():
    from pydiverse.pipedag.core import PipedagConfig

    with Flow("flow") as f:
        with Stage("stage"):
            _ = t("task")()

    # Kroki is disabled by default
    with PipedagConfig.default.get().evolve():
        visualization_url = f.visualize_url()
        assert visualization_url.startswith("<disable_kroki=True>/graphviz/")

    # Use kroki.io as default url
    with PipedagConfig.default.get().evolve(disable_kroki=False):
        visualization_url = f.visualize_url()
        assert visualization_url.startswith("https://kroki.io/graphviz/")

    # Check that overriding works
    with PipedagConfig.default.get().evolve(
        disable_kroki=False, kroki_url="THIS_IS_A_TEST_URL"
    ):
        visualization_url = f.visualize_url()
        assert visualization_url.startswith("THIS_IS_A_TEST_URL/graphviz/")
