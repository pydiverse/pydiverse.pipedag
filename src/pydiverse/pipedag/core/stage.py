from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

import structlog

from pydiverse.pipedag.context import ConfigContext, DAGContext
from pydiverse.pipedag.core.task import Task
from pydiverse.pipedag.errors import StageError
from pydiverse.pipedag.util import normalize_name

if TYPE_CHECKING:
    from pydiverse.pipedag.core.flow import Flow


class Stage:
    """A stage represents a collection of related tasks.

    The main purpose of a Stage is to allow for a transactionality mechanism.
    Only if all tasks inside a stage finish successfully does the stage
    get committed.

    All task that get defined inside the stage's context will automatically
    get added to the stage.

    :param name: The name of the stage. Two stages with the same name may
        not be used inside the same flow.

    ..
        To ensure that all tasks get executed in the correct order, each
        MaterializingTask must be an upstream dependency of its stage's
        CommitStageTask (this is done by calling the `add_task` method) and
        for each of its upstream dependencies, the associated StageCommitTask
        must be added as an upstream dependency.
        This ensures that the stage commit only happens after all tasks have
        finished writing to the transaction stage, and a task never gets executed
        before any of its upstream stage dependencies have been committed.
    """

    def __init__(self, name: str):
        self._name = normalize_name(name)
        self._transaction_name = f"{self._name}__tmp"

        self.tasks: list[Task] = []
        self.commit_task: CommitStageTask = None  # type: ignore
        self.outer_stage: Stage | None = None

        self.logger = structlog.get_logger(logger_name=type(self).__name__, stage=self)
        self.id: int = None  # type: ignore

        self._did_enter = False

    @property
    def name(self) -> str:
        """The name of the stage."""
        return self._name

    @property
    def transaction_name(self) -> str:
        """The name temporary transaction stage."""
        return self._transaction_name

    def set_transaction_name(self, new_transaction_name):
        # used by stage_commit_technique=READ_VIEWS to change odd/even
        # transaction schemas
        self._transaction_name = new_transaction_name

    @property
    def current_name(self) -> str:
        """The name of the stage where the data currently lives.

        Before a task has been committed this is the transaction name,
        after the commit it is the normal name.
        """

        if self.did_commit:
            return self.name
        else:
            return self.transaction_name

    @property
    def did_commit(self) -> bool:
        from pydiverse.pipedag.context.run_context import RunContext, StageState

        return RunContext.get().get_stage_state(self) == StageState.COMMITTED

    def __repr__(self):
        return f"<Stage: {self.name}>"

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("tasks", None)
        state.pop("commit_task", None)
        state.pop("logger", None)
        return state

    def __enter__(self):
        if self._did_enter:
            raise StageError(
                f"Stage '{self.name}' has already been entered."
                " Can't reuse the same stage twice."
            )
        self._did_enter = True

        # Capture information from surrounding Flow or Stage block
        # and link this stage with it
        try:
            outer_ctx = DAGContext.get()
        except LookupError as e:
            raise StageError("Stage can't be defined outside of a flow") from e

        outer_ctx.flow.add_stage(self)
        if outer_ctx.stage is not None:
            self.outer_stage = outer_ctx.stage

        # Initialize new context (both Flow and Stage use DAGContext to transport
        # information to @materialize annotations within the flow and to support
        # nesting of stages)
        self._ctx = DAGContext(
            flow=outer_ctx.flow,
            stage=self,
        )
        self._ctx.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit_task = CommitStageTask(self, self._ctx.flow)
        self._ctx.__exit__()
        del self._ctx

    def __getitem__(self, item: str | tuple[str, int]) -> Task:
        """Retrieves a task inside the stage by name.

        You can either subscribe a Stage using just a string (``stage["name"]``) or
        using a tuple containing a string and an integer (``stage["name", 3]``).

        The string is always interpreted as the name of the task to retrieve. If
        you also pass in an integer, it is interpreted as the `index` of the task.
        This means, that if the stage contains multiple tasks with the same name,
        then you can retrieve a specific instance of that task based on the order
        in which they were defined.

        :raises KeyError: If no task with the name can be found.
        :raises IndexError: If the index is out of bounds.
        :raises ValueError: If multiple matching tasks have been found, but no index
            has been provided.
        """

        if isinstance(item, str):
            name = item
            index = None
        elif isinstance(item, tuple):
            name, index = item
        else:
            raise TypeError

        tasks = [task for task in self.tasks if task.name == name]
        if not tasks:
            raise KeyError(
                f"Couldn't find a task with name '{name}' in stage '{self.name}'."
            )

        if index is None:
            if len(tasks) == 1:
                return tasks[0]

            raise ValueError(
                f"Found more than one task with name '{name}' in stage. "
                "Specify which task you want by passing in an index."
            )

        if index < len(tasks):
            return tasks[index]

        raise IndexError(
            f"Found only {len(tasks)} instances of task '{name}' in stage, "
            f"but you requested the tasks with index {index}, "
            "which is out of bounds."
        )

    def all_tasks(self):
        yield from self.tasks
        yield self.commit_task

    def is_inner(self, other: Stage):
        outer = self.outer_stage
        while outer is not None:
            if outer == other:
                return True
            outer = outer.outer_stage
        return False

    def get_task(self, name: str, index: int | None = None) -> Task:
        """Retrieves a task inside the stage by name.
        Alias for :py:meth:`Stage.__getitem__`.

        :param name: The name of the task to retrieve.
        :param index: If multiple task instances with the same name appear inside
            the stage, you can request a specific one by passing an index.
        """
        return self[name, index]


class CommitStageTask(Task):
    def __init__(self, stage: Stage, flow: Flow):
        # Because the CommitStageTask doesn't get added to the stage.tasks list,
        # we can't call the super initializer.
        self.name = f"Commit '{stage.name}'"
        self.nout = None

        self.logger = structlog.get_logger(logger_name="Commit Stage", stage=stage)

        self._bound_args = inspect.signature(self.fn).bind()
        self.flow = flow
        self.stage = stage

        self.flow.add_task(self)

        self.input_tasks = {}
        self.upstream_stages = [stage]

        self._skip_commit = len(stage.tasks) == 0
        self._visualize_hidden = True

    def fn(self):
        if self._skip_commit:
            return

        self.logger.info("Committing stage")
        ConfigContext.get().store.commit_stage(self.stage)
