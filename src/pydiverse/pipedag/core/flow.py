from __future__ import annotations

import base64
import random
from collections import defaultdict
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

import networkx as nx
import pydot
import structlog

from pydiverse.pipedag import ExternalTableReference, Table
from pydiverse.pipedag.context import (
    ConfigContext,
    DAGContext,
    FinalTaskState,
    RunContextServer,
)
from pydiverse.pipedag.context.context import CacheValidationMode
from pydiverse.pipedag.context.trace_hook import TraceHook
from pydiverse.pipedag.core.config import PipedagConfig
from pydiverse.pipedag.core.group_node import BarrierTask, VisualizationStyle
from pydiverse.pipedag.core.task import TaskGetItem
from pydiverse.pipedag.errors import DuplicateNameError, FlowError

if TYPE_CHECKING:
    from pydiverse.pipedag.core import GroupNode, Result, Stage, Task
    from pydiverse.pipedag.core.stage import CommitStageTask
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
        trace_hook: TraceHook = None,
        fail_fast: bool | None = None,
        cache_validation_mode: CacheValidationMode | None = None,
        disable_cache_function: bool | None = None,
        ignore_task_version: bool | None = None,
        ignore_position_hashes: bool = False,
        inputs: dict[Task | TaskGetItem, ExternalTableReference] | None = None,
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
        :param ignore_position_hashes:
            If ``True``, the position hashes of tasks are not checked
            when retrieving the inputs of a task from the cache.
            This simplifies execution of subgraphs if you don't care whether inputs to
            that subgraph are cache invalid. This allows multiple modifications in the
            Graph before the next run updating the cache.
            Attention: This may break automatic cache invalidation.
            And for this to work, any task producing an input
            for the chosen subgraph may never be used more
            than once per stage.
            NOTE: This is only supported for the SequentialEngine and SQLTablestore
        :param inputs:
            Optionally provide the outputs for a subset of tasks.
            The format is expected as
            dict[Task|TaskGetItem, ExternalTableReference].
            Every task that is listed in this mapping
            will not be executed but instead the output,
            will be read from the external reference.
            NOTE: Using this feature disables caching.
            NOTE: This feature is only supported when using
            the SQLTablestore at the moment.
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
        if ignore_position_hashes:
            self.logger.warn(
                "Using ignore_position_hashes=True! "
                "This may break automatic cache invalidation. "
                "And for this to work, any task producing an input "
                "for the chosen subgraph may never be used more than once per stage."
            )
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

        if trace_hook is None:
            trace_hook = TraceHook()

        # resolve constant inputs
        if inputs is not None:
            inputs_resolved = {}
            for task, ref in inputs.items():
                if isinstance(task, TaskGetItem):
                    inputs_resolved[(task.task.id, task.item)] = Table(ref)
                else:
                    inputs_resolved[(task.id, None)] = Table(ref)
        else:
            inputs_resolved = None

        with config, RunContextServer(subflow, trace_hook):
            if orchestration_engine is None:
                orchestration_engine = config.create_orchestration_engine()
            result = orchestration_engine.run(
                subflow, ignore_position_hashes, inputs_resolved, **kwargs
            )

            visualization_url = result.visualize_url()
            self.logger.info("Flow visualization", url=visualization_url)

        trace_hook.run_complete(result)

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
    def visualize(
        self, result: Result | None = None, visualization_tag: str | None = None
    ):
        """Visualizes the flow as a graph.

        If you are running in a jupyter notebook, the graph will get displayed inline.
        Otherwise, it will get rendered to a pdf that then gets opened in your browser.

        Requires `Graphviz <https://graphviz.org>`_ to be installed on your computer.

        :param result: An optional :py:class:`Result` instance.
            If provided, the visualization will contain additional information such
            as which tasks ran successfully, or failed.
        """
        return self.get_subflow().visualize(result, visualization_tag)

    def visualize_url(
        self, result: Result | None = None, visualization_tag: str | None = None
    ) -> str:
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
        return self.get_subflow().visualize_url(result, visualization_tag)

    def visualize_pydot(
        self, result: Result | None = None, visualization_tag: str | None = None
    ) -> pydot.Dot:
        """Visualizes the flow as a graph and return a ``pydot.Dot`` graph.

        :param result: An optional :py:class:`Result` instance.
            If provided, the visualization will contain additional information such
            as which tasks ran successfully, or failed.
        :return: A ``pydot.Dot`` graph.
        """
        return self.get_subflow().visualize_pydot(result, visualization_tag)


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

    def visualize(
        self, result: Result | None = None, visualization_tag: str | None = None
    ):
        dot = self.visualize_pydot(result, visualization_tag)
        _display_pydot(dot)
        return dot

    def visualize_url(
        self, result: Result | None = None, visualization_tag: str | None = None
    ) -> str:
        dot = self.visualize_pydot(result, visualization_tag)
        return _pydot_url(dot, result.config_context if result else None)

    def visualize_pydot(
        self, result: Result | None = None, visualization_tag: str | None = None
    ) -> pydot.Dot:
        from pydiverse.pipedag.core import GroupNode, Stage, Task

        flow = self.flow
        # Some group nodes are explicitly wired in the flow at declaration time, but
        # it is also possible to add group nodes via configuration. We must not modify
        # any objects in the flow to make them effective. Thus we create dictionaries
        # on the side to look up config created group nodes.
        task_group_nodes, stage_group_nodes, style_tags = _get_config_group_nodes(
            flow, result, visualization_tag
        )

        graph_boxes = set()  # type: Set[Stage | GroupNode]
        graph_nodes = set()  # type: Set[Task | GroupNode]
        graph_edges = set()  # type: Set[Tuple[Task | GroupNode, Task | GroupNode]]
        relevant_group_nodes = set()

        default_style = VisualizationStyle()

        def get_group_node_style(obj: GroupNode):
            return (
                obj.style if obj.style else style_tags.get(obj.style_tag, default_style)
            )

        def get_task_group_node(task: Task):
            return task_group_nodes.get(
                task, task.group_node if hasattr(task, "group_node") else None
            )

        def get_stage_outer_group_node(stage: Stage | GroupNode):
            return stage_group_nodes.get(stage, stage.outer_group_node)

        # add tasks and stages to graph and collect group nodes
        for task in self.get_tasks():
            if hasattr(task, "_visualize_hidden") and task._visualize_hidden:
                continue
            graph_boxes.add(task.stage)
            graph_nodes.add(task)
            if group_node := get_task_group_node(task):
                relevant_group_nodes.add(group_node)

            for input_task in task.input_tasks.values():
                graph_nodes.add(input_task)
                graph_boxes.add(input_task.stage)
                if group_node := get_task_group_node(input_task):
                    relevant_group_nodes.add(group_node)

        # add parent stages to graph
        for stage in graph_boxes.copy():
            if stage.outer_stage:
                graph_boxes.add(stage.outer_stage)

        # collect parent group nodes
        for stage in graph_boxes:
            if group_node := get_stage_outer_group_node(stage):
                relevant_group_nodes.add(group_node)

        # get group nodes including selected tasks recursively
        selected_group_nodes = set()
        for group_node in relevant_group_nodes:
            obj = group_node
            stage_tasks = {t for s in obj.stages for t in s.tasks}
            if (obj.tasks | stage_tasks) & self.selected_tasks:
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

        # hide all tasks within hidden group nodes
        for task in graph_nodes.copy():
            if group_node := get_task_group_node(task):
                if group_node.is_content_hidden(get_group_node_style):
                    graph_nodes.remove(task)

        # hide all stages within hidden group nodes
        for stage in graph_boxes.copy():
            if group_node := get_stage_outer_group_node(stage):
                if group_node.is_content_hidden(get_group_node_style):
                    graph_boxes.remove(stage)

        barrier_tasks = set()
        # add group nodes to graph and potentially hide stages and tasks
        for group_node in relevant_group_nodes:
            style = get_group_node_style(group_node)
            if (
                not group_node.outer_group_node
                or not group_node.outer_group_node.is_content_hidden(
                    get_group_node_style
                )
            ):
                if style.hide_box:
                    if group_node.entry_barrier_task:
                        # in-degree > 0 was already verified when creating
                        # entry_barrier_task
                        graph_nodes.add(group_node.entry_barrier_task)
                        barrier_tasks.add(group_node.entry_barrier_task)
                    if (
                        group_node.exit_barrier_task
                        and flow.explicit_graph.out_degree(group_node.exit_barrier_task)
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
            if group_node := get_task_group_node(task):
                if group_node.is_content_hidden(get_group_node_style):
                    obj = group_node
                    while (
                        obj.outer_group_node
                        and obj.outer_group_node.is_content_hidden(get_group_node_style)
                    ):
                        obj = obj.outer_group_node
                    return obj
            return task

        def add_edges(
            input_tasks: dict[int, Task] | Iterable[Task], target: Task | GroupNode
        ):
            if isinstance(input_tasks, dict):
                input_tasks = input_tasks.values()
            for input_task in input_tasks:
                input_node = get_node(input_task)
                if input_node in graph_nodes:
                    graph_edges.add((input_node, target))

        def add_group_node_edges(group_node: GroupNode):
            if group_node.entry_barrier_task:
                task = group_node.entry_barrier_task
                for prev_task in flow.explicit_graph.predecessors(task):
                    prev_node = get_node(prev_task)
                    if prev_node in graph_nodes:
                        graph_edges.add((prev_node, group_node))
            if group_node.exit_barrier_task:
                task = group_node.exit_barrier_task
                for next_task in flow.explicit_graph.successors(task):
                    next_node = get_node(next_task)
                    if next_node in graph_nodes:
                        graph_edges.add((group_node, next_node))

        for node in graph_nodes:
            if node in barrier_tasks:
                for prev_task in flow.explicit_graph.predecessors(node):
                    prev_node = get_node(prev_task)
                    if prev_node in graph_nodes:
                        graph_edges.add((prev_node, node))
                for next_task in flow.explicit_graph.successors(node):
                    next_node = get_node(next_task)
                    if next_node in graph_nodes:
                        graph_edges.add((node, next_node))
            elif isinstance(node, GroupNode):
                group_node = node
                content_hidden = group_node.is_content_hidden(get_group_node_style)
                for subtask in group_node.tasks:
                    target = group_node if content_hidden else subtask
                    if subtask in self.selected_tasks:
                        add_edges(
                            set(subtask.input_tasks.values()) - group_node.tasks, target
                        )
                add_group_node_edges(group_node)
            else:
                add_edges(node.input_tasks, node)

        for group_node in [b for b in graph_boxes if isinstance(b, GroupNode)]:
            content_hidden = group_node.is_content_hidden(get_group_node_style)
            for subtask in group_node.tasks:
                target = group_node if content_hidden else subtask
                if subtask in self.selected_tasks:
                    add_edges(subtask.input_tasks, target)
            if not get_group_node_style(group_node).hide_box:
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
            ) + (0.001 * s.id if isinstance(s, GroupNode) else 0)

        def task_sort_key(t):
            return (
                t.id
                if isinstance(t, Task)
                else min(t.id for t in t.tasks) - 0.1
                if t.tasks
                else min(stage_sort_key(s) for s in t.stages) - 0.01
            )

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
            relevant_group_nodes, result, get_group_node_style
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
            style = get_group_node_style(group_node)
            if not style.hide_box and group_node in group_node_style:
                if group_node.box_like_stage(style):
                    stage_style[group_node] = group_node_style[group_node]
                    if group_node_style[group_node].get("style") == "filled":
                        # included stages shall not inherit color from group node
                        for stage in group_node.stages:
                            stage_style[stage] = {
                                "style": "filled",
                                "fillcolor": "#dddddd",
                            }
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
            get_task_group_node=get_task_group_node,
            get_stage_outer_group_node=get_stage_outer_group_node,
            get_group_node_style=get_group_node_style,
            graph=graph,
            stage_style=stage_style,
            task_style=task_style,
            edge_style=edge_style,
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
            elif final_state == FinalTaskState.UNKNOWN:
                task_style[task] = {"fillcolor": "#ffffff"}
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
                    group_node_style[group_node] = {
                        "style": "filled",
                        "fillcolor": style.box_color_always,
                    }
                elif style.hide_content:
                    if FinalTaskState.FAILED in final_states:
                        fill_color = style.box_color_any_failure or "#ff453a"
                        group_node_style[group_node] = {
                            "style": "filled",
                            "fillcolor": fill_color,
                        }
                    elif final_states == {FinalTaskState.UNKNOWN}:
                        fill_color = style.box_color_all_skipped or "#ffffff"
                        group_node_style[group_node] = {
                            "style": "filled",
                            "fillcolor": fill_color,
                        }
                    elif FinalTaskState.CACHE_VALID not in final_states:
                        fill_color = style.box_color_none_cache_valid or "#adef9b"
                        group_node_style[group_node] = {
                            "style": "filled",
                            "fillcolor": fill_color,
                        }
                    elif final_states == {FinalTaskState.CACHE_VALID}:
                        fill_color = style.box_color_all_cache_valid or "#e8ffc6"
                        group_node_style[group_node] = {
                            "style": "filled",
                            "fillcolor": fill_color,
                        }
                    else:
                        fill_color = style.box_color_any_cache_valid or "#caffaf"
                        group_node_style[group_node] = {
                            "style": "filled",
                            "fillcolor": fill_color,
                        }
                else:
                    group_node_style[group_node] = {"style": "solid"}
    return group_node_style


def _build_pydot(
    stages: list[Stage | GroupNode],
    tasks: list[Task | GroupNode],
    get_task_group_node: Callable[[Task], GroupNode],
    get_stage_outer_group_node: Callable[[Stage], GroupNode],
    get_group_node_style: Callable[[GroupNode], VisualizationStyle],
    graph: nx.Graph,
    stage_style: dict[Stage | GroupNode, dict] | None,
    task_style: dict[Task | GroupNode, dict] | None,
    edge_style: dict[tuple[Task | GroupNode, Task | GroupNode], dict] | None,
) -> pydot.Dot:
    """
    Build a pydot graph from a graph of tasks and stages.

    :param stages:
        List of boxes around tasks be it stages or group nodes
    :param tasks:
        List of nodes in the graph be it tasks or group nodes
    :param get_task_group_node:
        Function to get the group node of a task. We can't use task.group_node directly
        because group nodes added by configuration should not modify the flow.
    :param get_stage_outer_group_node:
        Function to get the outer group node of a stage. We can't use
        stage.outer_group_node directly because group nodes added by configuration
        should not modify the flow.
    :param graph:
        The graph representing edges between tasks and stages
    :param stage_style:
        Style of boxes be it stages or group nodes
    :param task_style:
        Style of nodes in the graph be it tasks or group nodes
    :param edge_style:
        Style of edges in the graph
    :return:
        A pydot graph
    """
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
            group_node = stage
            label = group_node.label or ""
            if get_group_node_style(group_node).hide_label:
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
        stage_outer_group_node = get_stage_outer_group_node(stage)
        if (
            stage.outer_stage in stages
            and get_stage_outer_group_node(stage.outer_stage) is stage_outer_group_node
        ):
            subgraphs[stage.outer_stage].add_subgraph(s)
        elif stage_outer_group_node in stages and (
            not stage.outer_stage
            or stage.outer_stage not in stage_outer_group_node.stages
        ):
            subgraphs[stage_outer_group_node].add_subgraph(s)
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
            label = group_node.label or "|".join(
                sorted([t.name for t in group_node.stages])
                + sorted([t.name for t in group_node.tasks])
            )
            if get_group_node_style(group_node).hide_label:
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
            task_group_node = get_task_group_node(task)
            if (
                hasattr(task, "group_node")
                and task_group_node
                and task_group_node in stages
                and task.stage not in task_group_node.stages
            ):
                subgraphs[task_group_node].add_node(node)
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


def _pydot_url(dot: pydot.Dot, config: ConfigContext | None = None) -> str:
    import base64
    import zlib

    if config is None:
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


def _get_config_group_nodes(
    flow: Flow, result: Result | None, visualization_tag: str | None
):
    from pydiverse.pipedag.core import GroupNode

    logger = flow.logger
    if result:
        config_context = result.config_context
    else:
        try:
            config_context = ConfigContext.get()
        except LookupError:
            config_context = PipedagConfig.default.get(flow=flow.name)

    group_nodes, task_group_nodes, stage_group_nodes, style_tags = {}, {}, {}, {}
    if visualization_tag is None:
        visualization_tag = "default"
    if visualization_tag in config_context.visualization:
        visualization = config_context.visualization[visualization_tag]
        style_tags = visualization.styles or {}
        if visualization.group_nodes:
            for node_tag, node_config in visualization.group_nodes.items():
                style = (
                    visualization.styles.get(node_config.style_tag)
                    if visualization.styles
                    else None
                )
                group_node = GroupNode(
                    node_config.label, style, style_tag=node_config.style_tag
                )
                group_node.id = len(group_nodes) + len(flow.group_nodes)
                group_nodes[node_tag] = group_node
                for stage_name in node_config.stages or []:
                    if stage_name in flow.stages:
                        stage = flow.stages[stage_name]
                        group_node.add_stage(stage)
                        stage_group_nodes[stage] = group_node
                    else:
                        logger.error(
                            f"Stage {stage_name} in config based visualization "
                            f"group node {visualization_tag}.{node_tag} not found "
                            f"in flow."
                        )
                for stage in flow.stages.values():
                    if stage.group_node_tag == node_tag:
                        group_node.add_stage(stage)
                        stage_group_nodes[stage] = group_node

                for task_name in node_config.tasks or []:
                    found = False
                    for task in flow.tasks:
                        if task.name == task_name:
                            if found:
                                raise ValueError(
                                    f"Task {task_name} in config based "
                                    f"visualization group node "
                                    f"{visualization_tag}.{node_tag} appears "
                                    f"multiple times in flow."
                                )
                            found = True
                            if other_group_node := stage_group_nodes.get(task.stage):
                                if other_group_node is group_node:
                                    logger.info(
                                        f"Ignoring task {task_name} in config based"
                                        f" visualization group node "
                                        f"{visualization_tag}.{node_tag} because "
                                        f"its whole stage is included."
                                    )
                                    continue
                                if (
                                    group_node.outer_group_node
                                    and group_node.outer_group_node
                                    is not other_group_node
                                ):
                                    raise ValueError(
                                        f"Configured group node {node_tag} includes"
                                        f" tasks from multiple outer group nodes "
                                        f"which is not allowed: "
                                        f"{group_node.outer_group_node}, "
                                        f"{other_group_node}"
                                    )
                                group_node.outer_group_node = other_group_node
                                if (
                                    group_node.outer_stage
                                    and group_node.outer_stage not in group_node.stages
                                    and group_node.outer_stage
                                    is not other_group_node.outer_stage
                                ):
                                    raise ValueError(
                                        f"Configured group node {node_tag} includes"
                                        f" tasks from multiple stages which is not"
                                        f" allowed: "
                                        f"{group_node.outer_stage}, "
                                        f"{other_group_node.outer_stage}, "
                                        f"included_stages={group_node.stages}"
                                    )
                                group_node.outer_stage = other_group_node.outer_stage

                            if (
                                group_node.outer_stage
                                and group_node.outer_stage is not task.stage
                            ):
                                raise ValueError(
                                    f"Configured group node {node_tag} includes "
                                    f"tasks from multiple stages which is not "
                                    f"allowed: "
                                    f"{group_node.outer_stage}, {task.stage}"
                                )
                            group_node.outer_stage = task.stage
                            group_node.add_task(task)
                            if task.group_node:
                                logger.info(
                                    f"Adding task {task_name} to config based "
                                    f"visualization group node "
                                    f"{visualization_tag}.{node_tag}. The flow "
                                    f"based membership in {task.group_node} will "
                                    f"not take any effect (empty groups will not be"
                                    f" displayed)."
                                )
                            if task in task_group_nodes:
                                raise ValueError(
                                    f"Task {task_name} in config based "
                                    f"visualization group node "
                                    f"{visualization_tag}.{node_tag} appears"
                                    f" in multiple configured group nodes: "
                                    f"{task_group_nodes[task]}, {group_node}."
                                )
                            task_group_nodes[task] = group_node
                    if not found:
                        logger.error(
                            f"Task {task_name} in config based visualization "
                            f"group node {visualization_tag}.{node_tag} not found "
                            f"in flow."
                        )

                for task in flow.tasks:
                    if (
                        hasattr(task, "group_node_tag")
                        and task.group_node_tag == node_tag
                    ):
                        task_group_nodes[task] = group_node
                        group_node.add_task(task)

    elif visualization_tag != "default":
        logger.error(f"Visualization tag {visualization_tag} not found in config.")
    else:
        logger.info("No visualization customization found in config.")

    def find_outer_group_node(stage: Stage, stages: list[Stage]):
        if stage in stage_group_nodes:
            return stage_group_nodes[stage], stages + [stage]
        if stage.outer_stage:
            return find_outer_group_node(stage.outer_stage, stages + [stage])
        if stage.outer_group_node:
            return stage.outer_group_node, stages + [stage]
        return None, []

    def update_outer_group_node(stage: Stage):
        stage_outer_group_node, stages = find_outer_group_node(stage, [])
        if stage_outer_group_node:
            for stage in stages:
                stage_group_nodes[stage] = stage_outer_group_node
        return stage_outer_group_node

    # link tasks and stages to next outer group nodes
    for task in [t for t in flow.tasks if t not in task_group_nodes]:
        if hasattr(task, "group_node") and not task.group_node:
            if group_node := update_outer_group_node(task.stage):
                task_group_nodes[task] = group_node

    # link group nodes to outer group nodes and stages
    for group_node in group_nodes.values():
        outer_node = group_node.outer_group_node
        for task in group_node.tasks:
            outer_node = outer_node or update_outer_group_node(task.stage)
        for stage in group_node.stages:
            stage_outer_group_node = update_outer_group_node(stage)
            if stage_outer_group_node is not group_node:
                outer_node = outer_node or stage_outer_group_node
        group_node.outer_group_node = outer_node

    return task_group_nodes, stage_group_nodes, style_tags
