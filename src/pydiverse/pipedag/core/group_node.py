from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

from pydiverse.pipedag.context import DAGContext
from pydiverse.pipedag.core.task import Task
from pydiverse.pipedag.errors import GroupNodeError, StageError

if TYPE_CHECKING:
    from pydiverse.pipedag import Flow, Stage


@dataclass(frozen=True)
class VisualizationStyle:
    """
    Visualization style for group nodes.

    This can be configured via class GroupNode or via configuration in ``pipedag.yaml``.

    :param box: Box style of group node
    :param hide_content: Hide content of group node in visualization
    :param hide_label: Hide label of group node in visualization
    :param box_color_always: Color of group node box if specified
    :param box_color_any_failure: Color of group node box if any failure occurred within
        included tasks
    :param box_color_none_cache_valid: Color of group node box if no failure occurred
        and no included task cache valid
    :param box_color_any_cache_valid: Color of group node box if no failure occurred and
        some but not all included tasks cache valid
    :param box_color_all_cache_valid: Color of group node box if no failure occurred and
        all included tasks cache valid
    :param box_color_all_skipped: Color of group node box if no failure occurred and all
        included tasks were skipped
    """

    hide_box: bool = False
    hide_content: bool = False
    hide_label: bool = False
    box_color_always: str | None = None
    box_color_any_failure: str | None = None
    box_color_none_cache_valid: str | None = None
    box_color_any_cache_valid: str | None = None
    box_color_all_cache_valid: str | None = None
    box_color_all_skipped: str | None = None


class GroupNode:
    """A group node represents a collection of related tasks.

    The group can be used as a display element in the visualization, and it can be used
    to ensure all tasks before/after this group are executed before/after tasks in this
    group.

    Group nodes can contain stages and can be contained by stages.

    .. code-block:: python
       :caption: Example: How to group tasks with a group node.

        with Flow() as flow:
            with Stage("stage1"):
                _ = any_task()
                with GroupNode("group1"):
                    task1 = task_within_group()
                    _ = task_within_group2(task1)
                _ = any_task()

    https://kroki.io/graphviz/svg/eNqljk1LxDAQQO_5FUO8rtjUk5R4XdnjXhcJkyZtw45JyQeyiP_dbsOCyiKoOc7kvTfGjRHnCbbwxvrwMofijcyx2I6louuup5KyjSqplHG0Yvmqxz5QiJLfNOtrG96xOtKE_bFjQ_D5y4BQW5LVsMjziaxMgZz5VOKXlFfz7knst8N0F4Lk_ymOMZT5e1HAYXBEFyUaOzxovoGKZExH9ery5Lxa8Q1Ump8ha_hzx9rfGdorinfGmh8t6E_qbLrC3v8VXKICbh-hBTjQZNHI5TTK6EiKdf0Bc8SyVQ==

    :param label:
        label displayed in the visualization
    :param style:
        visualization style for this group node
    :param ordering_barrier:
        If True, a barrier task will be added to the stage before and after this group
        to ensure all tasks before/after this group are executed before/after tasks in
        this group.
    :param style_tag:
        Style tag to be used for visualization
    """

    def __init__(
        self,
        label: str | None = None,
        style: VisualizationStyle | None = None,
        *,
        ordering_barrier: bool = False,
        style_tag: str | None = None,
    ):
        self.label = label
        self.style = style
        self.ordering_barrier = ordering_barrier
        self.style_tag = style_tag

        self.stages: set[Stage] = set()
        self.tasks: set[Task] = set()
        self.prev_tasks: set[Task] = set()
        self.entry_barrier_task: BarrierTask = None  # type: ignore
        self.exit_barrier_task: BarrierTask = None  # type: ignore
        self.outer_stage: Stage | None = None
        self.outer_group_node: GroupNode | None = None
        self.id = None  # will be set by flow.add_group_node

        self.logger = structlog.get_logger(logger_name=type(self).__name__, group=self)

        self._did_enter = False

    def __repr__(self):
        return f"<GroupNode: {self.label}>"

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("tasks", None)
        state.pop("stages", None)
        state.pop("entry_barrier_task", None)
        state.pop("exit_barrier_task", None)
        state.pop("logger", None)
        return state

    def __enter__(self):
        if self._did_enter:
            raise GroupNodeError(
                f"GroupNode '{self.label}' has already been entered."
                " Can't reuse the same node twice."
            )
        self._did_enter = True

        # Capture information from surrounding Flow or Stage block
        # and link this stage with it
        try:
            outer_ctx = DAGContext.get()
        except LookupError as e:
            raise StageError("GroupNode can't be defined outside of a flow") from e

        outer_ctx.flow.add_group_node(self)
        if outer_ctx.stage is not None:
            self.outer_stage = outer_ctx.stage
            self.prev_tasks = set(self.outer_stage.tasks)
        if outer_ctx.group_node is not None:
            self.outer_group_node = outer_ctx.group_node

        # Initialize new context (both Flow and Stage use DAGContext to transport
        # information to @materialize annotations within the flow and to support
        # nesting of stages)
        self._ctx = DAGContext(
            flow=outer_ctx.flow,
            stage=outer_ctx.stage,
            group_node=self,
        )

        if self.ordering_barrier and self._ctx.stage is not None:
            if self._ctx.stage.tasks:
                self.entry_barrier_task = BarrierTask(
                    self, self._ctx.stage, self._ctx.flow, prefix="Entry "
                )
                self.outer_stage.barrier_tasks.append(self.entry_barrier_task)

        self._ctx.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.ordering_barrier and self._ctx.stage is not None:
            self.exit_barrier_task = BarrierTask(
                self, self._ctx.stage, self._ctx.flow, prefix="Exit "
            )
            self.outer_stage.barrier_tasks.append(self.exit_barrier_task)
        self._ctx.__exit__()
        del self._ctx

    def add_stage(self, stage: Stage):
        self.stages.add(stage)

    def add_task(self, task: Task):
        self.tasks.add(task)

    def is_content_hidden(self, get_style: Callable[[GroupNode], VisualizationStyle]):
        # recursive lookup for style.hide_content==True
        obj = self
        if get_style(obj).hide_content or False:
            return True
        if obj.outer_group_node is not None:
            return obj.outer_group_node.is_content_hidden(get_style)
        return False

    def box_like_stage(self, style: VisualizationStyle):
        return not (style.hide_content or False)


class BarrierTask(Task):
    def __init__(self, group_node: GroupNode, stage: Stage, flow: Flow, prefix=""):
        # Because the BarrierTask doesn't get added to the stage.tasks list,
        # we can't call the super initializer.
        self.prefix = prefix
        self.name = f"{prefix}Barrier '{stage.name}.{group_node.label}'"
        self.nout = None

        self.logger = structlog.get_logger(
            logger_name="Barrier", group_node=group_node, stage=stage
        )

        self._bound_args = inspect.signature(self.fn).bind()
        self.flow = flow
        self.stage = stage

        self.flow.add_task(self)

        self.input_tasks = {}
        self.upstream_stages = [stage]
        self._visualize_hidden = True

    def fn(self):
        self.logger.info(f"{self.prefix}Ordering barrier passed")
