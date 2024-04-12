from __future__ import annotations

import base64
import random
from collections import defaultdict
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

import networkx as nx
import pydot
import structlog

from pydiverse.pipedag.context import (
    ConfigContext,
    DAGContext,
    FinalTaskState,
    RunContextServer,
)
from pydiverse.pipedag.context.context import CacheValidationMode
from pydiverse.pipedag.core.config import PipedagConfig
from pydiverse.pipedag.core.group_node import BarrierTask, VisualizationStyle
from pydiverse.pipedag.errors import DuplicateNameError, FlowError

if TYPE_CHECKING:
    from pydiverse.pipedag.core import GroupNode, Result, Stage, Task
    from pydiverse.pipedag.core.stage import CommitStageTask
    from pydiverse.pipedag.core.task import TaskGetItem
    from pydiverse.pipedag.engine import OrchestrationEngine


class Flow:
    """
    A flow represents a collection of dependent Tasks.

    A flow is defined by using it as a context manager.
    Any stages and tasks defined inside the flow context will automatically get added
    to the flow with the correct dependency wiring.

    :param name: The name of the flow.

    Examples
    --------
    ::

        @materialize
        def my_materializing_task():
            return Table(...)

        with Flow("my_flow") as flow:
            with Stage("stage_1") as stage_1:
                task_1 = my_materializing_task()
                task_2 = another_materializing_task(task_1)

            with Stage("stage_2") as stage_2:
                task_3 = yet_another_task(task_3)
                ...

        result = flow.run()
    """

    def __init__(
        self,
        name: str = "default",
    ):
        self.name = name

        self.logger = structlog.get_logger(logger_name=type(self).__name__)
        self.stages: dict[str, Stage] = {}
        self.tasks: list[Task] = []
        self.group_nodes: list[GroupNode] = []

        self.graph = nx.DiGraph()
        self.explicit_graph: nx.DiGraph | None = None

    def __enter__(self):
        # Check that flows don't get nested
        try:
            DAGContext.get()
        except LookupError:
            pass
        else:
            raise RuntimeError("DAG Context already exists. Flows can't be nested.")

        # Initialize context (both Flow and Stage use DAGContext to transport
        # information to @materialize annotations within the flow and to
        # support nesting of stages)
        self._ctx = DAGContext(flow=self, stage=None, group_node=None)
        self._ctx.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._ctx.__exit__()
        del self._ctx

        self.explicit_graph = self.build_graph()

    def __getitem__(self, name) -> Stage:
        """Retrieves a stage by name.

        :param name: The name of the stage.
        :raises KeyError: if no stage with the specified name exists.
        """

        if stage := self.stages.get(name):
            return stage
        raise KeyError(f"Couldn't find a stage with name '{name}' in flow.")

    def add_stage(self, stage: Stage):
        if stage.name in self.stages:
            raise DuplicateNameError(f"Stage with name '{stage.name}' already exists.")

        stage.id = len(self.stages)
        self.stages[stage.name] = stage

    def add_group_node(self, group_node: GroupNode):
        group_node.id = len(self.group_nodes)
        self.group_nodes.append(group_node)

    def add_task(self, task: Task):
        assert self.stages[task.stage.name] is task.stage

        task.id = len(self.tasks)
        self.tasks.append(task)
        self.graph.add_node(task)

    def add_edge(self, from_: Task, to: Task):
        if from_ not in self.graph:
            raise FlowError(
                f"Can't add edge from {from_} to {to} because `from` is not in the"
                " flow."
            )
        if to not in self.graph:
            raise FlowError(
                f"Can't add edge from {from_} to {to} because `to` is not in the flow."
            )

        self.graph.add_edge(from_, to)

    def build_graph(self) -> nx.DiGraph:
        if not nx.is_directed_acyclic_graph(self.graph):
            raise FlowError("Graph is not a DAG")

        explicit_graph = self.graph.copy()
        stages = self.stages.values()
        group_nodes = self.group_nodes

        # Barrier Tasks
        for group_node in group_nodes:
            if group_node.entry_barrier_task is not None:
                explicit_graph.add_node(group_node.entry_barrier_task)
                # link entry barrier to leafs before
                before_leafs = group_node.prev_tasks
                for task in group_node.prev_tasks:
                    before_leafs = before_leafs - set(task.input_tasks.values())
                for task in before_leafs:
                    explicit_graph.add_edge(task, group_node.entry_barrier_task)
                # link barrier to source tasks within group node
                for task in group_node.tasks:
                    input_tasks = set(task.input_tasks.values())
                    if not (input_tasks & group_node.tasks):
                        explicit_graph.add_edge(group_node.entry_barrier_task, task)
            if group_node.exit_barrier_task is not None:
                all_tasks = set(group_node.outer_stage.tasks)
                after_tasks = all_tasks - group_node.tasks - group_node.prev_tasks
                if after_tasks:
                    explicit_graph.add_node(group_node.exit_barrier_task)
                    # link exit barrier to leaf tasks within group node
                    in_leafs = group_node.tasks
                    for task in group_node.tasks:
                        in_leafs = in_leafs - set(task.input_tasks.values())
                    for task in in_leafs:
                        explicit_graph.add_edge(task, group_node.exit_barrier_task)
                    # link exit barrier to source tasks after group bode
                    for task in after_tasks:
                        input_tasks = set(task.input_tasks.values())
                        if not (input_tasks & after_tasks):
                            explicit_graph.add_edge(group_node.exit_barrier_task, task)

        # Commit Tasks
        commit_tasks: dict[Stage, CommitStageTask] = {}
        for stage in stages:
            commit_tasks[stage] = stage.commit_task
            explicit_graph.add_node(stage.commit_task)

        # Add dependencies
        # Because stages can be nested, we add the commit task dependencies
        # to the leaf nodes of the subgraph
        contained_tasks: defaultdict[Stage, list[Task]] = defaultdict(lambda: [])
        leaf_nodes: defaultdict[Stage, list[Task]] = defaultdict(lambda: [])

        for stage in stages:
            s = stage
            while s is not None:
                contained_tasks[s].extend(stage.tasks)
                s = s.outer_stage

        for stage in stages:
            subgraph = self.graph.subgraph(contained_tasks[stage])  # type: nx.DiGraph
            for task, degree in subgraph.out_degree:  # type: ignore
                if degree != 0:
                    continue
                leaf_nodes[stage].append(task)

        # Add commit task downstream dependencies to leaf nodes
        for stage in stages:
            for leaf in leaf_nodes[stage]:
                commit_task = commit_tasks[stage]
                explicit_graph.add_edge(leaf, commit_task)

        # Add commit task upstream dependencies
        for task in self.tasks:
            for parent, _ in self.graph.in_edges(task):  # type: Task
                if parent.stage == task.stage:
                    continue
                if task.stage.is_inner(parent.stage):
                    continue

                commit_task = commit_tasks[parent.stage]
                explicit_graph.add_edge(commit_task, task)

        # Ensure inner stages get committed before outer stages
        for stage in stages:
            if stage.outer_stage is not None:
                explicit_graph.add_edge(
                    commit_tasks[stage], commit_tasks[stage.outer_stage]
                )

        return explicit_graph

    def get_subflow(self, *components: Task | TaskGetItem | Stage) -> Subflow:
        from pydiverse.pipedag import Stage, Task
        from pydiverse.pipedag.core.task import TaskGetItem

        tasks = []
        stages = []
        for component in components:
            if isinstance(component, Task):
                tasks.append(component)
            elif isinstance(component, TaskGetItem):
                tasks.append(component.task)
            elif isinstance(component, Stage):
                stages.append(component)
            else:
                raise TypeError(f"Unexpected object of type {type(component).__name__}")

        return Subflow(self, tasks=tasks, stages=stages)

    def run(
        self,
        *components: Task | TaskGetItem | Stage,
        config: ConfigContext = None,
        orchestration_engine: OrchestrationEngine = None,
        fail_fast: bool | None = None,
        cache_validation_mode: CacheValidationMode | None = None,
        disable_cache_function: bool | None = None,
        ignore_task_version: bool | None = None,
        **kwargs,
    ) -> Result:
        """Execute the flow.
        This will execute the flow and all tasks within using the orchestration engine.

        :param components: An optional selection of tasks or stages that should
            get executed. If no values are provided, all tasks and stages get executed.

            If you specify a subset of tasks, those tasks get executed even when
            they are cache valid, and they don't get committed.

            If you specify a subset of stages, all tasks and substages of those stages
            get executed and committed.
        :param config:
            A :py:class:`ConfigContext` to use as the configuration for executing
            this flow. If no config is provided, then pipedag uses the
            current innermost open ConfigContext, or if no such config exists,
            it gets the default config from :py:attr:`PipedagConfig.default`.
        :param orchestration_engine:
            The orchestration engine to use. If no engine is provided, the
            orchestration engine from the config gets used.
        :param fail_fast:
            Whether exceptions should get raised or swallowed.
            If set to True, exceptions that occur get immediately raised and the
            flow gets aborted.
        :param cache_validation_mode:
            Override the cache validation mode. See :py:class:`CacheValidationMode`.
            For None, the cache validation mode from the config gets used.
            See :doc:`/reference/config` :ref:`section-cache_validation` for more
            information.
        :param disable_cache_function:
            Override the disable_cache_function setting from the config.
            See :doc:`/reference/config` :ref:`section-cache_validation` for more
            information.
        :param ignore_task_version:
            Override the ignore_task_version setting from the config.
            See :doc:`/reference/config` :ref:`section-cache_validation` for more
            information.
        :param kwargs:
            Other keyword arguments that get passed on directly to the
            ``run()`` method of the orchestration engine. Consequently, these
            keyword arguments are engine dependant.
        :return:
            A :py:class:`Result` object for the current flow run.

        Examples
        --------
        .. code-block:: python

            with Flow() as flow:
                with Stage("stage_1") as stage_1:
                    task_1 = ...
                    task_2 = ...

                with Stage("stage_2") as stage_2:
                    task_3 = ...
                    task_4 = ...

            # Execute the entire flow
            flow.run()

            # Execute (and commit) only stage 1
            flow.run(stage_1)

            # Execute (but DON'T commit) only task 1 and task 4
            flow.run(task_1, task_4)

        """

        subflow = self.get_subflow(*components)

        # Get the ConfigContext to use
        if config is None:
            try:
                config = ConfigContext.get()
            except LookupError:
                config = PipedagConfig.default.get(flow=self.name)

        # update cache_validation_mode
        if cache_validation_mode is None and subflow.is_tasks_subflow:
            # If subflow consists of a subset of tasks (-> not a subset of stages)
            # then we want to skip cache validity checking to ensure the tasks
            # always get executed.
            cache_validation_mode = CacheValidationMode.FORCE_CACHE_INVALID

        # Evolve config using the arguments passed to flow.run
        config = config.evolve(
            fail_fast=(fail_fast if fail_fast is not None else config.fail_fast),
            cache_validation={
                k: v
                for k, v in [
                    ("mode", cache_validation_mode),
                    ("disable_cache_function", disable_cache_function),
                    ("ignore_task_version", ignore_task_version),
                ]
                if v is not None
            },
        )

        if (
            config.cache_validation.mode == CacheValidationMode.NORMAL
            and config.cache_validation.disable_cache_function
        ):
            raise ValueError(
                "disable_cache_function=True is not allowed in combination with "
                f"cache_validation_mode=NORMAL: {config.cache_validation}"
            )

        with config, RunContextServer(subflow):
            if orchestration_engine is None:
                orchestration_engine = config.create_orchestration_engine()
            result = orchestration_engine.run(subflow, **kwargs)

            visualization_url = result.visualize_url()
            self.logger.info("Flow visualization", url=visualization_url)

        if not result.successful and config.fail_fast:
            raise result.exception or Exception("Flow run failed")

        return result

    def get_stage(self, name: str) -> Stage:
        """Retrieves a stage by name.
        Alias for :py:meth:`Flow.__getitem__`.

        :param name: The name of the stage.
        :return: The stage.
        :raises KeyError: if no stage with the specified name exists.
        """

        return self[name]

    # Visualization
    def visualize(self, result: Result | None = None):
        """Visualizes the flow as a graph.

        If you are running in a jupyter notebook, the graph will get displayed inline.
        Otherwise, it will get rendered to a pdf that then gets opened in your browser.

        Requires `Graphviz <https://graphviz.org>`_ to be installed on your computer.

        :param result: An optional :py:class:`Result` instance.
            If provided, the visualization will contain additional information such
            as which tasks ran successfully, or failed.
        """
        dot = self.visualize_pydot(result)
        _display_pydot(dot)
        return dot

    def visualize_url(self, result: Result | None = None) -> str:
        """Visualizes the flow as a graph and returns a URL to view the visualization.

        If you don't have Graphviz installed on your computer (and thus aren't able to
        use the :py:meth:`~.visualize()` method) then this is the easiest way to
        visualize the flow.
        For this we use a free service called `Kroki <https://kroki.io>`_.

        :param result: An optional :py:class:`Result` instance.
            If provided, the visualization will contain additional information such
            as which tasks ran successfully, or failed.
        :return:
            A URL that, when opened, displays the graph.
        """
        dot = self.visualize_pydot(result)
        return _pydot_url(dot)

    def visualize_pydot(self, result: Result | None = None) -> pydot.Dot:
        """Visualizes the flow as a graph and return a ``pydot.Dot`` graph.

        :param result: An optional :py:class:`Result` instance.
            If provided, the visualization will contain additional information such
            as which tasks ran successfully, or failed.
        :return: A ``pydot.Dot`` graph.
        """
        subflow = self.get_subflow()
        return subflow.visualize_pydot(result)


class Subflow:
    def __init__(self, flow: Flow, tasks: list[Task], stages: list[Stage]):
        self.flow = flow
        self.name = flow.name
        self.is_tasks_subflow = len(tasks) > 0

        if tasks and stages:
            raise ValueError(
                "You can only specify either a subset of tasks OR subset of stages"
                " to run, but not both."
            )
        elif not tasks and not stages:
            self.selected_stages = set(flow.stages.values())
            self.selected_tasks = set(flow.tasks)
            return

        # Preprocessing
        self.selected_stages = set(stages)
        for stage in self.flow.stages.values():
            if stage.outer_stage in self.selected_stages:
                self.selected_stages.add(stage)

        # Construct set of selected tasks
        self.selected_tasks = set(tasks)

        if not tasks:
            for stage in self.selected_stages:
                for task in stage.tasks:
                    self.selected_tasks.add(task)
                self.selected_tasks.add(stage.commit_task)

    def get_tasks(self) -> Iterable[Task]:
        """
        All tasks that should get executed as part of this sub flow.
        Guaranteed to be in the same order as they were added to the flow
        (they will be ordered topologically).
        """
        graph = self.flow.explicit_graph
        for task in self.flow.tasks:
            if task in self.selected_tasks or (
                isinstance(task, BarrierTask)
                and (set(graph.predecessors(task)) | set(graph.successors(task)))
                & self.selected_tasks
            ):
                yield task

    def get_parent_tasks(self, task: Task) -> Iterable[Task]:
        """
        Returns tasks that must be executed before `task`.
        """
        if task not in self.selected_tasks and not isinstance(task, BarrierTask):
            return

        for parent_task, _ in self.flow.explicit_graph.in_edges(task):
            if parent_task in self.selected_tasks or isinstance(
                parent_task, BarrierTask
            ):
                yield parent_task

    def visualize(self, result: Result | None = None):
        dot = self.visualize_pydot(result)
        _display_pydot(dot)
        return dot

    def visualize_url(self, result: Result | None = None) -> str:
        dot = self.visualize_pydot(result)
        return _pydot_url(dot)

    def visualize_pydot(self, result: Result | None = None) -> pydot.Dot:
        from pydiverse.pipedag.core import GroupNode, Stage, Task

        graph_boxes = set()  # type: Set[Stage | GroupNode]
        graph_nodes = set()  # type: Set[Task | GroupNode]
        graph_edges = set()  # type: Set[Tuple[Task | GroupNode, Task | GroupNode]]
        relevant_group_nodes = set()

        default_style = VisualizationStyle()

        def get_style(obj: GroupNode):
            return obj.style if obj.style else default_style

        # add tasks and stages to graph and collect group nodes
        for node in self.get_tasks():
            if hasattr(node, "_visualize_hidden") and node._visualize_hidden:
                continue
            graph_boxes.add(node.stage)
            graph_nodes.add(node)
            if node.group_node:
                relevant_group_nodes.add(node.group_node)

            for input_task in node.input_tasks.values():
                graph_nodes.add(input_task)
                graph_boxes.add(input_task.stage)
                if input_task.group_node:
                    relevant_group_nodes.add(input_task.group_node)

        # add parent stages to graph
        for stage in graph_boxes.copy():
            if stage.outer_stage:
                graph_boxes.add(stage.outer_stage)

        # collect parent group nodes
        for stage in graph_boxes:
            if stage.outer_group_node:
                relevant_group_nodes.add(stage.outer_group_node)

        # get group nodes including selected tasks recursively
        selected_group_nodes = set()
        for group_node in relevant_group_nodes:
            obj = group_node
            if obj.tasks & self.selected_tasks:
                while True:
                    selected_group_nodes.add(obj)
                    obj = obj.outer_group_node
                    if obj is None:
                        break

        # add parent group nodes to graph
        for node in relevant_group_nodes.copy():
            parent = node.outer_group_node
            while parent:
                relevant_group_nodes.add(parent)
                parent = parent.outer_group_node

        barrier_tasks = set()
        # add group nodes to graph and potentially hide stages and tasks
        for group_node in relevant_group_nodes:
            style = get_style(group_node)
            if group_node.is_content_hidden(get_style):
                graph_nodes -= group_node.tasks
                graph_boxes -= group_node.stages
            if (
                not group_node.outer_group_node
                or not group_node.outer_group_node.is_content_hidden(get_style)
            ):
                if style.hide_box:
                    if group_node.entry_barrier_task:
                        # in-degree > 0 was already verified when creating
                        # entry_barrier_task
                        graph_nodes.add(group_node.entry_barrier_task)
                        barrier_tasks.add(group_node.entry_barrier_task)
                    if (
                        group_node.exit_barrier_task
                        and result.flow.explicit_graph.out_degree(
                            group_node.exit_barrier_task
                        )
                        > 0
                    ):
                        graph_nodes.add(group_node.exit_barrier_task)
                        barrier_tasks.add(group_node.exit_barrier_task)
                else:
                    if group_node.box_like_stage(style):
                        graph_boxes.add(group_node)
                    else:
                        graph_nodes.add(group_node)

        # add nodes and edges to graph
        graph = nx.DiGraph()

        def get_node(task: Task) -> Task | GroupNode:
            if task.group_node:
                if task.group_node.is_content_hidden(get_style):
                    obj = task.group_node
                    while (
                        obj.outer_group_node
                        and obj.outer_group_node.is_content_hidden(get_style)
                    ):
                        obj = obj.outer_group_node
                    return obj
            return task

        def add_edges(input_tasks: dict[int, Task], target: Task | GroupNode):
            for input_task in input_tasks.values():
                input_node = get_node(input_task)
                if input_node in graph_nodes:
                    graph_edges.add((input_node, target))

        def add_group_node_edges(group_node: GroupNode):
            if group_node.entry_barrier_task:
                task = group_node.entry_barrier_task
                for prev_task in result.flow.explicit_graph.predecessors(task):
                    prev_node = get_node(prev_task)
                    if prev_node in graph_nodes:
                        graph_edges.add((prev_node, group_node))
            if group_node.exit_barrier_task:
                task = group_node.exit_barrier_task
                for next_task in result.flow.explicit_graph.successors(task):
                    next_node = get_node(next_task)
                    if next_node in graph_nodes:
                        graph_edges.add((group_node, next_node))

        for node in graph_nodes:
            if node in barrier_tasks:
                for prev_task in result.flow.explicit_graph.predecessors(node):
                    prev_node = get_node(prev_task)
                    if prev_node in graph_nodes:
                        graph_edges.add((prev_node, node))
                for next_task in result.flow.explicit_graph.successors(node):
                    next_node = get_node(next_task)
                    if next_node in graph_nodes:
                        graph_edges.add((node, next_node))
            elif isinstance(node, GroupNode):
                group_node = node
                content_hidden = group_node.is_content_hidden(get_style)
                for subtask in group_node.tasks:
                    target = group_node if content_hidden else subtask
                    if subtask in self.selected_tasks:
                        add_edges(subtask.input_tasks, target)
                add_group_node_edges(group_node)
            else:
                add_edges(node.input_tasks, node)

        for group_node in [b for b in graph_boxes if isinstance(b, GroupNode)]:
            content_hidden = group_node.is_content_hidden(get_style)
            for subtask in group_node.tasks:
                target = group_node if content_hidden else subtask
                if subtask in self.selected_tasks:
                    add_edges(subtask.input_tasks, target)
            if not get_style(group_node).hide_box:
                add_group_node_edges(group_node)

        # canonically sort boxes, nodes, and edges
        def stage_sort_key(s):
            return (
                float(s.id)
                if isinstance(s, Stage)
                else stage_sort_key(s.outer_group_node) + 0.01
                if s.outer_group_node
                else s.outer_stage.id + 0.1
                if s.outer_stage
                else min(s2.id for s2 in s.stages) - 0.1
            )

        def task_sort_key(t):
            return t.id if isinstance(t, Task) else t.id + 1e6

        def edge_sort_key(e):
            return (task_sort_key(e[0]), task_sort_key(e[1]))

        sorted_boxes = sorted(graph_boxes, key=stage_sort_key)
        sorted_nodes = sorted(graph_nodes, key=task_sort_key)
        sorted_edges = sorted(graph_edges, key=edge_sort_key)

        # fill graph
        for node in sorted_nodes:
            graph.add_node(node)
        for node1, node2 in sorted_edges:
            graph.add_edge(node1, node2)

        # prepare styles
        stage_style = {}
        task_style = _generate_task_style(graph_nodes, result)
        group_node_style = _generate_group_node_style(
            relevant_group_nodes, result, get_style
        )
        edge_style = {}

        for stage in graph_boxes:
            if stage not in self.selected_stages:
                stage_style[stage] = {
                    "style": '"dashed"',
                    "bgcolor": "#0000000A",
                    "color": "#909090",
                    "fontcolor": "#909090",
                }

        for node in graph_nodes:
            if node not in self.selected_tasks and node not in barrier_tasks:
                task_style[node] = {
                    "style": '"filled,dashed"',
                    "fillcolor": "#FFFFFF80",
                    "color": "#808080",
                    "fontcolor": "#909090",
                }

        for group_node in relevant_group_nodes:
            style = get_style(group_node)
            if not style.hide_box:
                if group_node.box_like_stage(style):
                    stage_style[group_node] = group_node_style[group_node]
                else:
                    task_style[group_node] = group_node_style[group_node]
            if group_node not in selected_group_nodes and not style.hide_box:
                if group_node.box_like_stage(style):
                    stage_style[group_node] = {
                        "style": '"dashed"',
                        "bgcolor": "#0000000A",
                        "color": "#909090",
                        "fontcolor": "#909090",
                    }
                else:
                    task_style[group_node] = {
                        "style": '"filled,dashed"',
                        "fillcolor": "#FFFFFF80",
                        "color": "#808080",
                        "fontcolor": "#909090",
                    }

        for edge in graph.edges:
            if (
                edge[0] not in self.selected_tasks
                and edge[0] not in barrier_tasks
                and (
                    not isinstance(edge[0], GroupNode)
                    or all(t not in self.selected_tasks for t in edge[0].tasks)
                )
            ):
                edge_style[edge] = {
                    "style": "dashed",
                    "color": "#909090",
                }

        return _build_pydot(
            stages=list(sorted_boxes),
            tasks=list(sorted_nodes),
            graph=graph,
            stage_style=stage_style,
            task_style=task_style,
            edge_style=edge_style,
            get_style=get_style,
        )


# Visualization Helper


def _generate_task_style(tasks: Iterable[Task], result: Result | None) -> dict:
    task_style = {}
    if result:
        for task in tasks:
            final_state = result.task_states.get(task, FinalTaskState.UNKNOWN)
            if final_state == FinalTaskState.COMPLETED:
                task_style[task] = {"fillcolor": "#adef9b"}
            elif final_state == FinalTaskState.CACHE_VALID:
                task_style[task] = {"fillcolor": "#e8ffc6"}
            elif final_state == FinalTaskState.FAILED:
                task_style[task] = {"fillcolor": "#ff453a"}
            elif final_state == FinalTaskState.SKIPPED:
                task_style[task] = {"fillcolor": "#fccb83"}
    return task_style


def _generate_group_node_style(
    group_nodes: Iterable[GroupNode],
    result: Result | None,
    get_style: Callable[[GroupNode], VisualizationStyle],
) -> dict:
    group_node_style = {}
    if result:
        for group_node in group_nodes:
            style = get_style(group_node)
            final_states = {
                result.task_states.get(task, FinalTaskState.UNKNOWN)
                for task in group_node.tasks
            }
            if not style.hide_box:
                if style.box_color_always:
                    group_node_style[group_node] = {"fillcolor": style.box_color_always}
                elif FinalTaskState.FAILED in final_states:
                    fill_color = style.box_color_any_failure or "#ff453a"
                    group_node_style[group_node] = {"fillcolor": fill_color}
                elif final_states == {FinalTaskState.SKIPPED}:
                    fill_color = style.box_color_all_skipped or "#fccb83"
                    group_node_style[group_node] = {"fillcolor": fill_color}
                elif FinalTaskState.CACHE_VALID not in final_states:
                    fill_color = style.box_color_none_cache_valid or "#adef9b"
                    group_node_style[group_node] = {"fillcolor": fill_color}
                elif final_states == {FinalTaskState.CACHE_VALID}:
                    fill_color = style.box_color_all_cache_valid or "#e8ffc6"
                    group_node_style[group_node] = {"fillcolor": fill_color}
                else:
                    fill_color = style.box_color_any_cache_valid or "#caffff"
                    group_node_style[group_node] = {"fillcolor": fill_color}
    return group_node_style


def _build_pydot(
    stages: list[Stage | GroupNode],
    tasks: list[Task | GroupNode],
    graph: nx.Graph,
    stage_style: dict[Stage | GroupNode, dict] | None,
    task_style: dict[Task | GroupNode, dict] | None,
    edge_style: dict[tuple[Task | GroupNode, Task | GroupNode], dict] | None,
    get_style: Callable[[GroupNode], VisualizationStyle],
) -> pydot.Dot:
    from pydiverse.pipedag.core import GroupNode

    if stage_style is None:
        stage_style = {}
    if task_style is None:
        task_style = {}
    if edge_style is None:
        edge_style = {}

    dot = pydot.Dot(compound="true")
    subgraphs: dict[Stage | GroupNode, pydot.Subgraph] = {}
    nodes: dict[Task | GroupNode, pydot.Node] = {}

    for stage in stages:
        style = dict(
            style="solid",
            bgcolor="#00000020",
            color="black",
            fontcolor="black",
        ) | (stage_style.get(stage, {}))

        if isinstance(stage, GroupNode):
            label = stage.label or ""
            if get_style(stage).hide_label:
                label = ""
            s = pydot.Cluster(
                f"n_{base64.b64encode(random.randbytes(8)).decode('ascii')}",
                label=label,
                **style,
            )
        else:
            s = pydot.Cluster(f"s_{stage.name}", label=stage.name, **style)
        subgraphs[stage] = s

    for stage in stages:
        s = subgraphs[stage]
        if stage.outer_group_node in stages and (
            not stage.outer_stage
            or stage.outer_stage not in stage.outer_group_node.stages
        ):
            subgraphs[stage.outer_group_node].add_subgraph(s)
        elif stage.outer_stage in stages:
            subgraphs[stage.outer_stage].add_subgraph(s)
        else:
            dot.add_subgraph(s)

    for task in tasks:
        style = dict(
            fillcolor="#FFFFFF",
            style='"filled"',
        ) | (task_style.get(task, {}))

        if isinstance(task, GroupNode):
            group_node = task
            label = group_node.label or "|".join([t.name for t in group_node.tasks])
            if get_style(group_node).hide_label:
                label = ""
            node = pydot.Node(
                base64.b64encode(random.randbytes(8)).decode("ascii"),
                label=label,
                **style,
            )
            if group_node.outer_group_node in stages and (
                not group_node.outer_stage
                or group_node.outer_stage not in group_node.outer_group_node.stages
            ):
                subgraphs[group_node.outer_group_node].add_node(node)
            elif group_node.outer_stage in stages:
                subgraphs[group_node.outer_stage].add_node(node)
            else:
                dot.add_node(node)
        else:
            node = pydot.Node(task.id, label=task.name, **style)
            if (
                hasattr(task, "group_node")
                and task.group_node
                and task.group_node in stages
                and task.stage not in task.group_node.stages
            ):
                subgraphs[task.group_node].add_node(node)
            else:
                subgraphs[task.stage].add_node(node)
        nodes[task] = node

    for nx_edge in graph.edges:
        style = edge_style.get(nx_edge, {})
        sender = nodes[nx_edge[0]] if nx_edge[0] in nodes else subgraphs[nx_edge[0]]
        receiver = nodes[nx_edge[1]] if nx_edge[1] in nodes else subgraphs[nx_edge[1]]
        sender_task = (
            sender if nx_edge[0] in nodes else nodes[list(nx_edge[0].tasks)[0]]
        )
        receiver_task = (
            receiver if nx_edge[1] in nodes else nodes[list(nx_edge[1].tasks)[0]]
        )
        edge = pydot.Edge(
            sender_task,
            receiver_task,
            ltail=sender.get_name(),
            lhead=receiver.get_name(),
            **style,
        )
        dot.add_edge(edge)

    return dot


def _display_pydot(dot: pydot.Dot):
    """Display a pydot.Dot graph

    - Either as svg in ipython
    - Or as PDF in default pdf viewer
    """

    try:
        from IPython import get_ipython
        from IPython.display import SVG, display

        ipython = get_ipython()
        if ipython is None or ipython.config.get("IPKernelApp") is None:
            raise RuntimeError("Either IPython isn't running or SVG aren't supported.")
        display(SVG(dot.create_svg()))  # type: ignore
    except (ImportError, RuntimeError):
        import tempfile
        import webbrowser

        f = tempfile.NamedTemporaryFile(suffix=".pdf")
        f.write(dot.create_pdf())  # type: ignore
        webbrowser.open_new("file://" + f.name)

        # Keep alive to prevent immediate deletion of file
        globals()["__pipedag_tmp_file_reference__"] = f


def _pydot_url(dot: pydot.Dot) -> str:
    import base64
    import zlib

    try:
        config = ConfigContext.get()
    except LookupError:
        config = PipedagConfig.default.get()

    kroki_url = config.kroki_url
    if config.disable_kroki or kroki_url is None:
        kroki_url = "<disable_kroki=True>"

    query_data = zlib.compress(str(dot).encode("utf-8"), 9)
    query = base64.urlsafe_b64encode(query_data).decode("ascii")

    return f"{kroki_url}/graphviz/svg/{query}"
