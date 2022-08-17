from __future__ import annotations

import copy
import inspect
from typing import TYPE_CHECKING, Any, Callable

import structlog

from pydiverse.pipedag.context import (
    ConfigContext,
    DAGContext,
    RunContextProxy,
    TaskContext,
)
from pydiverse.pipedag.errors import FlowError, StageError
from pydiverse.pipedag.util import deepmutate

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Flow, Stage


class Task:
    def __init__(
        self,
        fn: Callable,
        *,
        name: str = None,
        nout: int = None,
        pass_task: bool = False,
    ):
        if not callable(fn):
            raise TypeError("`fn` must be callable")

        if name is None:
            name = getattr(fn, "__name__", type(self).__name__)

        self.fn = fn
        self.name = name
        self.nout = nout
        self.pass_task = pass_task

        self.flow: Flow = None  # type: ignore
        self.stage: Stage = None  # type: ignore
        self.upstream_stages: list[Stage] = None  # type: ignore
        self.input_tasks: dict[int, Task] = None  # type: ignore

        self._signature = inspect.signature(fn)
        self._bound_args: inspect.BoundArguments = None  # type: ignore

        self.logger = structlog.get_logger()

        self.value = None
        self.id: int = None  # type: ignore

    def __repr__(self):
        return f"<Task '{self.name}' {hex(id(self))}>"

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
            raise FlowError("Can't call pipedag task outside of a flow.")

        if ctx.stage is None:
            raise StageError("Can't call pipedag task outside of a stage.")

        new = copy.copy(self_)
        new.flow = ctx.flow
        new.stage = ctx.stage

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

        deepmutate(new._bound_args.args, visitor)
        deepmutate(new._bound_args.kwargs, visitor)

        # Add upstream edges
        upstream_stages_set = set()
        for input_task in new.input_tasks.values():
            new.flow.add_edge(input_task, new)
            upstream_stages_set.add(input_task.stage)

        new.upstream_stages = list(upstream_stages_set)

        return new

    def run(
        self,
        inputs: dict[int, Any],
        run_context: RunContextProxy = None,
        config_context: ConfigContext = None,
    ):
        # Hand over run context if using multiprocessing
        if run_context is None:
            run_context = RunContextProxy.get()
        if config_context is None:
            config_context = ConfigContext.get()

        # With multiprocessing the task value doesn't get shared between
        # processes. That's why we must take a dict of inputs as arguments
        # (when running with multiprocessing) and update the value of all
        # relevant tasks.
        for task_id, value in inputs.items():
            task = self.input_tasks[task_id]
            task.value = value

        with run_context, config_context:
            try:
                result = self._run()
            except Exception as e:
                self.on_failure()
                raise e
            else:
                self.on_success()
                return result

    def _run(self):
        args = self._bound_args.args
        kwargs = self._bound_args.kwargs

        def task_result_mutator(x):
            if isinstance(x, Task):
                return x.value
            elif isinstance(x, TaskGetItem):
                return x.value
            return x

        args = deepmutate(args, task_result_mutator)
        kwargs = deepmutate(kwargs, task_result_mutator)

        with TaskContext(task=self):
            result = self.fn(*args, **kwargs)
        self.value = result

        return result

    # Run Metadata

    def prepare_for_run(self):
        for stage in self.upstream_stages:
            stage.incr_ref_count()

    def on_success(self):
        self.logger.info("Task finished successfully", task=self)
        for stage in self.upstream_stages:
            stage.decr_ref_count()

    def on_failure(self):
        self.logger.warning("Task failed", task=self)
        for stage in self.upstream_stages:
            stage.decr_ref_count()


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

    @property
    def value(self):
        if self.parent.value is None:
            raise TypeError(f"Parent ({self.parent}) value is None.")
        return self.parent.value[self.item]
