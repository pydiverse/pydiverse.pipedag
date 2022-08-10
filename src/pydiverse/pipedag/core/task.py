from __future__ import annotations

import copy
import functools
import inspect
from typing import TYPE_CHECKING, Any, Callable

from pydiverse.pipedag.context import DAGContext
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
    ):
        if not callable(fn):
            raise TypeError("`fn` must be callable")

        if name is None:
            name = getattr(fn, "__name__", type(self).__name__)

        self.fn = fn
        self.name = name
        self.nout = nout

        functools.update_wrapper(self, self.fn)

        self.flow: Flow = None  # type: ignore
        self.stage: Stage = None  # type: ignore
        self.upstream_stages: list[Stage] = None  # type: ignore
        self.input_tasks: list[Task] = None  # type: ignore

        self._signature = inspect.signature(fn)
        self._bound_args: inspect.BoundArguments = None  # type: ignore

        self.value = None

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
            return TaskGetItem(self, self, i)

    def __call__(self_, *args, **kwargs):
        """Do the wiring"""
        new = copy.copy(self_)

        ctx = DAGContext.get()
        new.flow = ctx.flow
        new.stage = ctx.stage

        new._bound_args = new._signature.bind(*args, **kwargs)

        new.flow.add_task(new)
        new.stage.tasks.append(new)

        # Compute input tasks
        new.input_tasks = []

        def mutator(x):
            if isinstance(x, Task):
                new.input_tasks.append(x)
            elif isinstance(x, TaskGetItem):
                new.input_tasks.append(x.task)
            return x

        deepmutate(new._bound_args.args, mutator)
        deepmutate(new._bound_args.kwargs, mutator)

        # Add upstream edges
        upstream_stages_set = set()
        for input_task in new.input_tasks:
            new.flow.add_edge(input_task, new)
            upstream_stages_set.add(input_task.stage)

        new.upstream_stages = list(upstream_stages_set)

        return new

    def run(self):
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

        result = self.fn(*args, **kwargs)
        self.value = result

        return result

    def get_input_tasks(self, args, kwargs):
        input_tasks = []

        def mutator(x):
            if isinstance(x, Task):
                input_tasks.append(x)
            elif isinstance(x, TaskGetItem):
                input_tasks.append(x.task)
            return x

        deepmutate(args, mutator)
        deepmutate(kwargs, mutator)

        return input_tasks

    # Run Metadata

    def prepare_for_run(self):
        for stage in self.upstream_stages:
            stage.incr_ref_count()

    def on_success(self):
        for stage in self.upstream_stages:
            stage.decr_ref_count()

    def on_failure(self):
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
        return self.parent.value[self.item]
