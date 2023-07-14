from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
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
from pydiverse.pipedag.core.config import PipedagConfig
from pydiverse.pipedag.errors import DuplicateNameError, FlowError

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Result, Stage, Task
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
        self._ctx = DAGContext(flow=self, stage=None)
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
        from pydiverse.pipedag.core.stage import Stage
        from pydiverse.pipedag.core.task import Task, TaskGetItem

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
        ignore_fresh_input: bool = False,
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
        :param ignore_fresh_input:
            When set to True, the task's cache function gets ignored when determining
            the cache validity of a task.
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

        # Evolve config using the arguments passed to flow.run
        config = config.evolve(
            fail_fast=(fail_fast if fail_fast is not None else config.fail_fast),
            ignore_fresh_input=ignore_fresh_input,
            ignore_task_version=(
                # If subflow consists of a subset of tasks (-> not a subset of stages)
                # then we want to ignore the task version to ensure the tasks always
                # get executed.
                subflow.is_tasks_subflow
                or config.ignore_task_version
            ),
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
        task_style = _generate_task_style(self.tasks, result)
        dot = _build_pydot(
            stages=list(self.stages.values()),
            tasks=self.tasks,
            graph=self.graph,
            task_style=task_style,
        )

        return dot


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
        for task in self.flow.tasks:
            if task in self.selected_tasks:
                yield task

    def get_parent_tasks(self, task: Task) -> Iterable[Task]:
        """
        Returns tasks that must be executed before `task`.
        """
        if task not in self.selected_tasks:
            return

        for parent_task, _ in self.flow.explicit_graph.in_edges(task):
            if parent_task in self.selected_tasks:
                yield parent_task

    def visualize(self, result: Result | None = None):
        dot = self.visualize_pydot(result)
        _display_pydot(dot)
        return dot

    def visualize_url(self, result: Result | None = None) -> str:
        dot = self.visualize_pydot(result)
        return _pydot_url(dot)

    def visualize_pydot(self, result: Result | None = None) -> pydot.Dot:
        graph = nx.DiGraph()

        relevant_stages = set()
        relevant_tasks = set()

        for task in self.get_tasks():
            graph.add_node(task)
            relevant_stages.add(task.stage)
            relevant_tasks.add(task)

            for input_task in task.input_tasks.values():
                relevant_stages.add(input_task.stage)
                relevant_tasks.add(input_task)

                if input_task in self.selected_tasks:
                    graph.add_node(input_task)
                graph.add_edge(input_task, task)

        stage_style = {}
        task_style = _generate_task_style(relevant_tasks, result)
        edge_style = {}

        for stage in relevant_stages:
            if stage not in self.selected_stages:
                stage_style[stage] = {
                    "style": '"dashed"',
                    "bgcolor": "#0000000A",
                    "color": "#909090",
                    "fontcolor": "#909090",
                }

        for task in relevant_tasks:
            if task not in self.selected_tasks:
                task_style[task] = {
                    "style": '"filled,dashed"',
                    "fillcolor": "#FFFFFF80",
                    "color": "#808080",
                    "fontcolor": "#909090",
                }

        for edge in graph.edges:
            if edge[0] not in self.selected_tasks:
                edge_style[edge] = {
                    "style": "dashed",
                    "color": "#909090",
                }

        return _build_pydot(
            stages=sorted(relevant_stages, key=lambda s: s.id),
            tasks=list(relevant_tasks),
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
            elif final_state == FinalTaskState.SKIPPED:
                task_style[task] = {"fillcolor": "#fccb83"}
    return task_style


def _build_pydot(
    stages: list[Stage],
    tasks: list[Task],
    graph: nx.Graph,
    stage_style: dict[Stage, dict] = None,
    task_style: dict[Task, dict] = None,
    edge_style: dict[tuple[Task, Task], dict] = None,
) -> pydot.Dot:
    if stage_style is None:
        stage_style = {}
    if task_style is None:
        task_style = {}
    if edge_style is None:
        edge_style = {}

    dot = pydot.Dot()
    subgraphs: dict[Stage, pydot.Subgraph] = {}
    nodes: dict[Task, pydot.Node] = {}

    for stage in stages:
        style = dict(
            style="solid",
            bgcolor="#00000020",
            color="black",
            fontcolor="black",
        ) | (stage_style.get(stage, {}))

        s = pydot.Subgraph(f"cluster_{stage.name}", label=stage.name, **style)
        subgraphs[stage] = s

        if stage.outer_stage in stages:
            subgraphs[stage.outer_stage].add_subgraph(s)
        else:
            dot.add_subgraph(s)

    for task in tasks:
        if task._visualize_hidden:
            continue

        style = dict(
            fillcolor="#FFFFFF",
            style='"filled"',
        ) | (task_style.get(task, {}))

        node = pydot.Node(task.id, label=task.name, **style)
        nodes[task] = node
        subgraphs[task.stage].add_node(node)

    for nx_edge in graph.edges:
        style = edge_style.get(nx_edge, {})
        edge = pydot.Edge(nodes[nx_edge[0]], nodes[nx_edge[1]], **style)
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

    query_data = zlib.compress(str(dot).encode("utf-8"), 9)
    query = base64.urlsafe_b64encode(query_data).decode("ascii")

    return f"https://kroki.io/graphviz/svg/{query}"
