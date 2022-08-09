import networkx as nx

from pydiverse.pipedag import Flow, Stage
from pydiverse.pipedag.core import Task


def t(name: str):
    return Task((lambda *a: a), name=f"task-{name}")


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


def test_one_task():
    with Flow("f") as f:
        with Stage("s") as s:
            t0 = t("0")(0)

    validate_dependencies(f)


def test_two_independent_tasks():
    with Flow("f") as f:
        with Stage("s") as s:
            t0 = t("0")(0)
            t1 = t("1")(1)

    validate_dependencies(f)


def test_two_connected_tasks():
    with Flow("f") as f:
        with Stage("s") as s:
            t0 = t("0")(0)
            t1 = t("1")(t0)

    validate_dependencies(f)


def test_two_stages():
    with Flow("f") as f:
        with Stage("stage 0") as s0:
            t00 = t("00")(0)
            t01 = t("01")(t00)

        with Stage("stage 1") as s1:
            t10 = t("10")(t00)
            t11 = t("11")(t10, t00)
            t12 = t("12")(t00)

    validate_dependencies(f)


def test_nested():
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

    validate_dependencies(f)
