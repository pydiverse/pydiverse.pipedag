from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from pydiverse.pipedag.context import ConfigContext, DAGContext
from pydiverse.pipedag.core.task import Task
from pydiverse.pipedag.errors import StageError
from pydiverse.pipedag.util import normalize_name

if TYPE_CHECKING:
    from pydiverse.pipedag.core.flow import Flow


class Stage:
    """The Stage class is used to group a collection of related tasks

    The main purpose of a Stage is to allow for a transactionality mechanism.
    Only if all tasks inside a stage finish successfully does the stage
    get committed.

    All task that get defined inside the stage's context will automatically
    get added to the stage.

    An example of how to use a Stage:
    ::

        @materialize()
        def my_materializing_task():
            return Table(...)

        with Flow("my_flow") as flow:
            with Stage("stage_1") as stage_1:
                task_1 = my_materializing_task()
                task_2 = another_materializing_task(task_1)

            with Stage("stage_2") as stage_2:
                task_3 = yet_another_task(task_3)
                ...

        flow.run()

    To ensure that all tasks get executed in the correct order, each
    MaterializingTask must be an upstream dependency of its stage's
    CommitStageTask (this is done by calling the `add_task` method) and
    for each of its upstream dependencies, the associated StageCommitTask
    must be added as an upstream dependency.
    This ensures that the stage commit only happens after all tasks have
    finished writing to the transaction stage, and a task never gets executed
    before any of its upstream stage dependencies have been committed.

    :param name: The name of the stage. Two stages with the same name may
        not be used inside the same flow.
    """

    def __init__(self, name: str):
        self._name = normalize_name(name)
        self._transaction_name = f"{self._name}__tmp"

        self.tasks: list[Task] = []
        self.commit_task: CommitStageTask = None  # type: ignore
        self.outer_stage: Stage | None = None

        self.logger = structlog.get_logger(stage=self)
        self.id: int = None  # type: ignore

        self._did_enter = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def transaction_name(self) -> str:
        return self._transaction_name

    @property
    def current_name(self) -> str:
        """The name of the stage where the data currently lives

        Before a task has been committed this is the transaction name,
        after the commit it is the base name.
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

    def __enter__(self):
        if self._did_enter:
            raise StageError(
                f"Stage '{self.name}' has already been entered."
                " Can't reuse the same stage twice."
            )
        self._did_enter = True

        # Capture information from surrounding Flow or Stage block and link this stage with it
        try:
            outer_ctx = DAGContext.get()
        except LookupError as e:
            raise StageError(f"Stage can't be defined outside of a flow") from e

        outer_ctx.flow.add_stage(self)
        if outer_ctx.stage is not None:
            self.outer_stage = outer_ctx.stage

        # Initialize new context (both Flow and Stage use DAGContext to transport information to @materialize
        #   annotations within the flow and to support nesting of stages)
        self._ctx = DAGContext(
            flow=outer_ctx.flow,
            stage=self,
        )
        self._ctx.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit_task = CommitStageTask(self, self._ctx.flow)
        self._ctx.flow.add_task(self.commit_task)
        self._ctx.__exit__()
        del self._ctx

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

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("tasks", None)
        state.pop("commit_task", None)
        state.pop("outer_stage", None)
        state.pop("logger", None)
        return state


class CommitStageTask(Task):
    def __init__(self, stage: Stage, flow: Flow):
        super().__init__(
            fn=self.fn,
            name=f"Commit '{stage.name}'",
        )

        self.stage = stage
        self.flow = flow
        self.upstream_stages = {stage}
        self.input_tasks = {}

        self._bound_args = self._signature.bind()
        self._visualize_hidden = True

    def fn(self):
        self.logger.info(f"Committing stage '{self.stage.name}'")
        ConfigContext.get().store.commit_stage(self.stage)
