from __future__ import annotations

import inspect
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Callable

import structlog

from pydiverse.pipedag.context import ConfigContext, DAGContext, RunContext, TaskContext
from pydiverse.pipedag.context.run_context import FinalTaskState
from pydiverse.pipedag.errors import FlowError, StageError
from pydiverse.pipedag.util import deep_map
from pydiverse.pipedag.util.hashing import stable_hash

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Flow, Stage


class UnboundTask:
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

        self._bound_task_type = Task
        self._signature = inspect.signature(fn)

    def __repr__(self):
        return f"<UnboundTask 'name' {hex(id(self))}>"

    def __call__(self, *args, **kwargs) -> Task:
        """Constructs a `Task` with bound inputs"""
        try:
            ctx = DAGContext.get()
        except LookupError:
            raise FlowError("Can't call pipedag task outside of a flow.") from None

        if ctx.stage is None:
            raise StageError("Can't call pipedag task outside of a stage.")

        # Construct Task
        bound_args = self._signature.bind(*args, **kwargs)
        return self._bound_task_type(self, bound_args, ctx.flow, ctx.stage)


class Task:
    def __init__(
        self,
        unbound_task: UnboundTask,
        bound_args: inspect.BoundArguments,
        flow: Flow,
        stage: Stage,
    ):
        self.fn = unbound_task.fn
        self.name = unbound_task.name
        self.nout = unbound_task.nout

        self.logger = structlog.get_logger(logger_name=f"Task '{self.name}'", task=self)

        self._bound_args = bound_args
        self.flow = flow
        self.stage = stage
        self.position_hash = self.__compute_position_hash()

        # The ID gets set by calling flow.add_task
        self.id: int = None  # type: ignore

        # Do the wiring
        self.flow.add_task(self)
        self.stage.tasks.append(self)

        # Compute input tasks
        self.input_tasks: dict[int, Task] = {}

        def visitor(x):
            if isinstance(x, Task):
                self.input_tasks[x.id] = x
            elif isinstance(x, TaskGetItem):
                self.input_tasks[x.task.id] = x.task
            return x

        deep_map(bound_args.args, visitor)
        deep_map(bound_args.kwargs, visitor)

        # Add upstream edges
        self.upstream_stages: list[Stage] = []

        upstream_stages_set = set()
        for input_task in self.input_tasks.values():
            self.flow.add_edge(input_task, self)

            if input_task.stage not in upstream_stages_set:
                upstream_stages_set.add(input_task.stage)
                self.upstream_stages.append(input_task.stage)

        # Private flags
        self._visualize_hidden = False

    def __repr__(self):
        return f"<Task '{self.name}' {hex(id(self))} (id: {self.id})>"

    def __hash__(self):
        return id(self)

    def __getitem__(self, item) -> TaskGetItem:
        return TaskGetItem(self, self, item)

    def __iter__(self) -> Iterable[TaskGetItem]:
        if self.nout is None:
            raise ValueError("Can't iterate over task without specifying `nout`.")
        for i in range(self.nout):
            yield TaskGetItem(self, self, i)

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
                result, task_context = self._run(inputs)
            except Exception as e:
                if config_context._swallow_exceptions:
                    # PIPEDAG INTERNAL
                    self.logger.info("Task failed (raised an exception)", cause=str(e))
                else:
                    self.logger.exception("Task failed (raised an exception)")
                self.did_finish(FinalTaskState.FAILED)
                raise e
            else:
                if task_context.is_cache_valid:
                    self.did_finish(FinalTaskState.CACHE_VALID)
                else:
                    self.did_finish(FinalTaskState.COMPLETED)
                return result

    def _run(self, inputs: [int, Any]) -> tuple[Any, TaskContext]:
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

        with TaskContext(task=self) as task_context:
            result = self.fn(*args, **kwargs)

        return result, task_context

    def __compute_position_hash(self) -> str:
        """
        The position hash encodes the position & wiring of a task within a flow.
        If two tasks have the same position hash, this means that their inputs are
        derived in the same way.
        """

        from pydiverse.pipedag.util.json import PipedagJSONEncoder

        def visitor(x):
            if isinstance(x, (Task, TaskGetItem)):
                return x.position_hash
            return x

        arguments = deep_map(self._bound_args.arguments, visitor)
        input_json = PipedagJSONEncoder().encode(arguments)

        return stable_hash("POS_TASK", self.name, self.stage.name, input_json)

    def did_finish(self, state: FinalTaskState):
        if state.is_successful():
            self.logger.info("Task finished successfully", state=state)
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

        self.position_hash = stable_hash(
            "POS_GET_ITEM", parent.position_hash, repr(self.item)
        )

    def __getitem__(self, item):
        return type(self)(self.task, self, item)

    def resolve_value(self, task_value: Any):
        parent_value = self.parent.resolve_value(task_value)
        if parent_value is None:
            raise TypeError(f"Parent ({self.parent}) value is None.")
        return parent_value[self.item]
