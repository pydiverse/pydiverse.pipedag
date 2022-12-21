from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

import networkx as nx
import structlog

from pydiverse.pipedag.context import ConfigContext, DAGContext, RunContextServer
from pydiverse.pipedag.errors import DuplicateNameError, FlowError
from pydiverse.pipedag.util.config import PipedagConfig

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Result, Stage, Task
    from pydiverse.pipedag.core.stage import CommitStageTask
    from pydiverse.pipedag.engine import OrchestrationEngine


class Flow:
    def __init__(
        self,
        name: str = "default",
    ):
        self.name = name

        self.logger = structlog.getLogger(module=__name__, cls=self.__class__.__name__)
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

    # noinspection PyPackageRequirements
    def visualize(self):
        # TODO: Also allow visualizing the run result
        #       Successful tasks in green, failed in red, skipped orange
        import pydot

        dot = pydot.Dot()
        subgraphs: dict[Stage, pydot.Subgraph] = {}
        nodes: dict[Task, pydot.Node] = {}

        for stage in self.stages.values():
            s = pydot.Subgraph(
                f"cluster_{stage.name}",
                label=stage.name,
                bgcolor="#00000011",
            )
            subgraphs[stage] = s

            if stage.outer_stage is None:
                dot.add_subgraph(s)
            else:
                subgraphs[stage.outer_stage].add_subgraph(s)

        for task in self.tasks:
            # noinspection PyProtectedMember
            if task._visualize_hidden:
                continue

            node = pydot.Node(
                task.id,
                label=task.name,
                fillcolor="#FFFFFF",
                style="filled",
            )
            nodes[task] = node
            subgraphs[task.stage].add_node(node)

        for nx_edge in self.graph.edges:
            edge = pydot.Edge(nodes[nx_edge[0]], nodes[nx_edge[1]])
            dot.add_edge(edge)

        # Display
        # Either as svg in ipython
        # Or as PDF in default pdf viewer
        try:
            # noinspection PyUnresolvedReferences
            from IPython import get_ipython

            # noinspection PyUnresolvedReferences
            from IPython.display import SVG, display

            ipython = get_ipython()
            if ipython is None or ipython.config.get("IPKernelApp") is None:
                raise RuntimeError(
                    "Either IPython isn't running or SVG aren't supported."
                )

            display(SVG(dot.create_svg()))  # type: ignore
        except (ImportError, RuntimeError):
            import tempfile
            import webbrowser

            f = tempfile.NamedTemporaryFile(suffix=".pdf")
            f.write(dot.create_pdf())  # type: ignore
            webbrowser.open_new("file://" + f.name)

            # Keep alive to prevent immediate deletion of file
            globals()["__pipedag_tmp_file_reference__"] = f

        return dot

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

    def run(
        self,
        config_context: ConfigContext = None,
        orchestration_engine: OrchestrationEngine = None,
        fail_fast: bool | None = None,
        ignore_fresh_input: bool = False,
        stages: list[str] = None,
        **kwargs,
    ) -> Result:
        """Execute a flow

        You can provide an engine to execute the flow with using the `engine`
        keyword. If no engine is provided, the engine specified in the config
        file is used.

        :param config_context: A configuration context with information about
            the pipe-DAG instance.
        :param orchestration_engine: The orchestration engine to use.
        :param fail_fast: True means that errors should be raised as exceptions
            out of this function.
        :param ignore_fresh_input: whether cache functions should be disabled that
            check for fresh input into pipe-DAG
        :param kwargs: Other keyword arguments. They get passed on directly to the
            orchestration engine's `.run` method and thus are engine dependant.
        :return:
            Result object that gives information whether run was successful
        """

        # TODO: implement running only a subset of stages (see parameter stages)

        # Get the ConfigContext to use
        if config_context is None:
            try:
                config_context = ConfigContext.get()
            except LookupError:
                config_context = PipedagConfig.default.get()

        with config_context, RunContextServer(self) as run_context:
            # Configure Run Context
            run_context.ignore_fresh_input = ignore_fresh_input

            if orchestration_engine is None:
                orchestration_engine = config_context.create_orchestration_engine()
            result = orchestration_engine.run(flow=self, **kwargs)

        fail_fast = config_context.fail_fast if fail_fast is None else fail_fast
        if not result.successful and fail_fast:
            raise result.exception or Exception("Flow run failed")

        return result
