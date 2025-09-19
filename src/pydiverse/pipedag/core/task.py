# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import inspect
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Callable

import structlog

from pydiverse.common.util import deep_map
from pydiverse.common.util.hashing import stable_hash
from pydiverse.pipedag import Table
from pydiverse.pipedag.context import ConfigContext, DAGContext, RunContext, TaskContext
from pydiverse.pipedag.context.run_context import FinalTaskState
from pydiverse.pipedag.errors import StageError

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
        name: str | None = None,
        nout: int | None = None,
        group_node_tag: str | None = None,
        call_context: Callable[[], Any] | None = None,
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
        self.group_node_tag = group_node_tag
        self.call_context = call_context

        self._bound_task_type = Task
        self._signature = inspect.signature(fn)

    def __repr__(self):
        return f"<UnboundTask '{self.name}' {hex(id(self))}>"

    def __call__(self, *args, **kwargs) -> "Task":
        """
        When called inside a flow definition context:
        Constructs a `Task` with bound inputs

        When called outside a flow definition context:
        Invoke to original function.
        """
        try:
            ctx = DAGContext.get()
        except LookupError:
            return self._call_original_function(*args, **kwargs)

        if ctx.stage is None:
            raise StageError("Can't call pipedag task outside of a stage.")

        # Construct Task
        bound_args = self._signature.bind(*args, **kwargs)
        task = self._bound_task_type(self, bound_args, ctx.flow, ctx.stage)
        if ctx.group_node is not None:
            ctx.group_node.add_task(task)
            task._group_node = ctx.group_node
        return task

    def _call_original_function(self, *args, **kwargs):
        return self.fn(*args, **kwargs)


class TaskGetItem:
    """
    Wrapper __getitem__ on tasks
    """

    def __init__(self, task: "Task", parent: "Task | TaskGetItem", item: Any, *, is_member_lookup: bool = False):
        self._task = task
        self._parent = parent
        self._item = item
        self._is_member_lookup = is_member_lookup

        self._position_hash = stable_hash("POS_GET_ITEM", parent._position_hash, repr(self._item))

    def __getitem__(self, item):
        return type(self)(self._task, self, item)

    def _resolve_value(self, task_value: Any):
        parent_value = self._parent._resolve_value(task_value)
        if parent_value is None:
            raise TypeError(f"Parent ({self._parent}) value is None.")
        if self._is_member_lookup:
            return getattr(parent_value, self._item)
        else:
            return parent_value[self._item]


class Task:
    def __init__(
        self,
        unbound_task: UnboundTask,
        bound_args: inspect.BoundArguments,
        flow: "Flow",
        stage: "Stage",
    ):
        self._fn = unbound_task.fn
        self._name = unbound_task.name
        self._nout = unbound_task.nout
        self._group_node_tag = unbound_task.group_node_tag
        self._call_context = unbound_task.call_context

        self._logger = structlog.get_logger(logger_name=f"Task '{self._name}'", task=self)

        self._bound_args = bound_args
        self._flow = flow
        self._stage = stage
        self._group_node = None  # will be set by UnboundTask.__call__
        self._position_hash = self.__compute_position_hash()

        # The ID gets set by calling flow.add_task
        self._id: int = None  # type: ignore

        # Do the wiring
        self._flow.add_task(self)
        self._stage.tasks.append(self)

        # Compute input tasks
        self._input_tasks: dict[int, Task] = {}

        # Flag is set if table was materialized for debugging.
        # Setting this flag will fail the task.
        self.debug_tainted = False

        def visitor(x):
            if isinstance(x, Task):
                self._input_tasks[x._id] = x
            elif isinstance(x, TaskGetItem):
                self._input_tasks[x._task._id] = x._task
            return x

        deep_map(bound_args.args, visitor)
        deep_map(bound_args.kwargs, visitor)

        # Add upstream edges
        self._upstream_stages: list[Stage] = []

        upstream_stages_set = set()
        for input_task in self._input_tasks.values():
            self._flow.add_edge(input_task, self)

            if input_task._stage not in upstream_stages_set:
                upstream_stages_set.add(input_task._stage)
                self._upstream_stages.append(input_task._stage)

        # Private flags
        self._visualize_hidden = False

    def __repr__(self):
        return f"<Task '{self._name}' (id: {self._id})>"

    def __hash__(self):
        return id(self)

    def __getitem__(self, item) -> TaskGetItem:
        return TaskGetItem(self, self, item)

    def __iter__(self) -> Iterable[TaskGetItem]:
        if self._nout is None:
            raise ValueError("Can't iterate over task without specifying `nout`.")
        for i in range(self._nout):
            yield TaskGetItem(self, self, i)

    def _do_run(
        self,
        inputs: dict[int, Any],
        run_context: RunContext = None,
        config_context: ConfigContext = None,
        ignore_position_hashes: bool = False,
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
                    self._logger.info("Task failed (raised an exception)", cause=str(e))
                else:
                    self._logger.exception("Task failed (raised an exception)")
                self._did_finish(FinalTaskState.FAILED)
                raise e
            else:
                if task_context.is_cache_valid:
                    self._did_finish(FinalTaskState.CACHE_VALID)
                else:
                    self._did_finish(FinalTaskState.COMPLETED)
                return result

    def _run(self, inputs: [int, Any]) -> tuple[Any, TaskContext]:
        args = self._bound_args.args
        kwargs = self._bound_args.kwargs

        # Because tasks don't contain any state, we must retrieve the value
        # it returned from the inputs dictionary. This is especially important
        # because with multiprocessing it is impossible to even share state.
        def task_result_mapper(x):
            if isinstance(x, Task):
                return x._resolve_value(inputs[x._id])
            if isinstance(x, TaskGetItem):
                if isinstance(inputs[x._task._id], Table):
                    return x._task._resolve_value(inputs[x._task._id])
                else:
                    return x._resolve_value(inputs[x._task._id])
            return x

        args = deep_map(args, task_result_mapper)
        kwargs = deep_map(kwargs, task_result_mapper)

        with TaskContext(task=self) as task_context:
            if hasattr(self, "_call_context") and self._call_context:
                with self._call_context():
                    result = self._fn(*args, **kwargs)
            else:
                result = self._fn(*args, **kwargs)
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
                return x._position_hash
            return x

        arguments = deep_map(self._bound_args.arguments, visitor)
        input_json = PipedagJSONEncoder().encode(arguments)

        return stable_hash("POS_TASK", self._name, self._stage.name, input_json)

    def _did_finish(self, state: FinalTaskState):
        if state.is_successful():
            self._logger.info("Task finished successfully", state=state)
        RunContext.get().did_finish_task(self, state)

    def _resolve_value(self, task_value: Any):
        return task_value
