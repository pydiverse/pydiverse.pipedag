from __future__ import annotations

import copy
import inspect
from typing import TYPE_CHECKING, Any, Callable

import structlog

from pydiverse.pipedag.context import ConfigContext, DAGContext, RunContext, TaskContext
from pydiverse.pipedag.context.run_context import FinalTaskState
from pydiverse.pipedag.errors import FlowError, StageError
from pydiverse.pipedag.util import deep_map

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Flow, Stage


class Task:
    """Main building block of flows

    Tasks are just fancy functions that can be used inside a flow. As a user
    of pipedag you most likely never interact with the Task class directly,
    but use the @materialize decorator to create a MaterializingTask instead.

    :param fn: The function that gets executed by this task.
    :param name: The name of the task. If no name is specified, the name of
        `fn` is used instead.
    :param nout: The number out outputs that this task has. This is only used
        for unpacking the outputs of a task.
    """

    def __init__(
        self,
        fn: Callable,
        *,
        name: str = None,
        nout: int = None,
    ):
        if not callable(fn):
            raise TypeError("`fn` must be callable")
        if nout is not None and (nout <= 0 or not isinstance(nout, int)):
            raise ValueError("`nout` must be a positive integer")

        if name is None:
            name = getattr(fn, "__name__", type(self).__name__)

        self.fn = fn
        self.name = name
        self.nout = nout

        self.id: int = None  # type: ignore
        self.flow: Flow = None  # type: ignore
        self.stage: Stage = None  # type: ignore
        self.upstream_stages: list[Stage] = None  # type: ignore
        self.input_tasks: dict[int, Task] = None  # type: ignore

        self._signature = inspect.signature(fn)
        self._bound_args: inspect.BoundArguments = None  # type: ignore

        self.logger = structlog.get_logger()
        self._visualize_hidden = False

    def __repr__(self):
        return f"<Task '{self.name}' {hex(id(self))} (id: {self.id})>"

    def __hash__(self):
        return id(self)

    def __getitem__(self, item):
        return TaskGetItem(self, self, item)

    def __iter__(self):
        if self.nout is None:
            raise ValueError("Can't iterate over task without specifying `nout`.")
        for i in range(self.nout):
            yield TaskGetItem(self, self, i)

    def __call__(self_, *args, **kwargs):
        """Do the wiring"""
        try:
            ctx = DAGContext.get()
        except LookupError:
            raise FlowError("Can't call pipedag task outside of a flow.") from None

        if ctx.stage is None:
            raise StageError("Can't call pipedag task outside of a stage.")

        new = copy.copy(self_)
        new.flow = ctx.flow
        new.stage = ctx.stage
        new.logger = structlog.get_logger(task=new)

        new._bound_args = new._signature.bind(*args, **kwargs)

        new.flow.add_task(new)
        new.stage.tasks.append(new)

        # Compute input tasks
        new.input_tasks = {}

        def visitor(x):
            if isinstance(x, Task):
                new.input_tasks[x.id] = x
            elif isinstance(x, TaskGetItem):
                new.input_tasks[x.task.id] = x.task
            return x

        deep_map(new._bound_args.args, visitor)
        deep_map(new._bound_args.kwargs, visitor)

        # Add upstream edges
        new.upstream_stages = []
        upstream_stages_set = set()
        for input_task in new.input_tasks.values():
            new.flow.add_edge(input_task, new)

            if input_task.stage not in upstream_stages_set:
                upstream_stages_set.add(input_task.stage)
                new.upstream_stages.append(input_task.stage)

        return new

    def run(
        self,
        inputs: dict[int, Any],
        run_context: RunContext = None,
        config_context: ConfigContext = None,
    ):
        # Hand over run context if using multiprocessing
        if run_context is None:
            run_context = RunContext.get()
        if config_context is None:
            config_context = ConfigContext.get()

        with run_context, config_context:
            try:
                result = self._run(inputs)
            except Exception as e:
                self.did_finish(FinalTaskState.FAILED)
                raise e
            else:
                self.did_finish(FinalTaskState.COMPLETED)
                return result

    def _run(self, inputs: [int, Any]):
        args = self._bound_args.args
        kwargs = self._bound_args.kwargs

        # Because tasks don't contain any state, we must retrieve the value
        # it returned from the inputs dictionary. This is especially important
        # because with multiprocessing it is impossible to even share state.
        def task_result_mapper(x):
            if isinstance(x, Task):
                return x.resolve_value(inputs[x.id])
            if isinstance(x, TaskGetItem):
                return x.resolve_value(inputs[x.task.id])
            return x

        args = deep_map(args, task_result_mapper)
        kwargs = deep_map(kwargs, task_result_mapper)

        with TaskContext(task=self):
            result = self.fn(*args, **kwargs)

        return result

    def did_finish(self, state: FinalTaskState):
        if state == FinalTaskState.COMPLETED:
            self.logger.info("Task finished successfully", task=self, state=state)
        else:
            self.logger.warning("Task failed", task=self, state=state)
        RunContext.get().did_finish_task(self, state)

    def resolve_value(self, task_value: Any):
        return task_value


class TaskGetItem:
    """
    Wrapper __getitem__ on tasks
    """

    def __init__(self, task: Task, parent: Task | TaskGetItem, item: Any):
        self.task = task
        self.parent = parent
        self.item = item

    def __getitem__(self, item):
        return TaskGetItem(self.task, self, item)

    def resolve_value(self, task_value: Any):
        parent_value = self.parent.resolve_value(task_value)
        if parent_value is None:
            raise TypeError(f"Parent ({self.parent}) value is None.")
        return parent_value[self.item]
