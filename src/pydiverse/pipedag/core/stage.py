from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Callable

from attrs import frozen

from pydiverse.pipedag.context import DAGContext
from pydiverse.pipedag.core.task import Task
from pydiverse.pipedag.errors import StageError
from pydiverse.pipedag.util import normalise_name

if TYPE_CHECKING:
    from pydiverse.pipedag.core.flow import Flow


class Stage:
    def __init__(self, name: str):
        self._name = normalise_name(name)
        self._transaction_name = f"{self._name}__tmp"

        self.tasks: list[Task] = []
        self.commit_task: CommitStageTask = None  # type: ignore
        self.outer_stage: Stage | None = None

        # Reference Counting & Co
        self.__lock = threading.Lock()
        self.__ref_count = 0
        self.__ref_count_free_handler: Callable[[Stage], None] | None = None
        self.__did_enter = False
        self.__did_commit = False

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
        with self.__lock:
            return self.__did_commit

    def __repr__(self):
        return f"<Stage: {self.name}>"

    def __enter__(self):
        with self.__lock:
            if self.__did_enter:
                raise StageError(
                    f"Stage '{self.name}' has already been entered."
                    " Can't reuse the same stage twice."
                )
            self.__did_enter = True

        outer_ctx = DAGContext.get()
        outer_ctx.flow.add_stage(self)

        if outer_ctx.stage is not None:
            self.outer_stage = outer_ctx.stage

        self._ctx = DAGContext(
            flow=outer_ctx.flow,
            stage=self,
        )
        self._ctx.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit_task = CommitStageTask(self, self._ctx.flow)
        self._ctx.__exit__()

    def is_inner(self, other: Stage):
        outer = self.outer_stage
        while outer is not None:
            if outer == other:
                return True
            outer = outer.outer_stage
        return False

    def prepare_for_run(self):
        self.__did_commit = False

        # Reset reference counter
        if self.__ref_count != 0 and self.__ref_count_free_handler is not None:
            self.__ref_count = 0
            self.__ref_count_free_handler(self)

        # Increase reference counter
        for task in self.tasks:
            task.prepare_for_run()
        self.commit_task.prepare_for_run()

    # Reference Counting

    @property
    def ref_count(self):
        """The current reference counter value"""
        with self.__lock:
            return self.__ref_count

    def incr_ref_count(self, by: int = 1):
        with self.__lock:
            self.__ref_count += by
            print("----------------------------------", self, self.__ref_count)

    def decr_ref_count(self, by: int = 1):
        with self.__lock:
            self.__ref_count -= by
            print("----------------------------------", self, self.__ref_count)
            assert self.__ref_count >= 0

            if self.__ref_count == 0 and callable(self.__ref_count_free_handler):
                self.__ref_count_free_handler(self)

    def set_ref_count_free_handler(self, handler: Callable[[Stage], None] | None):
        assert callable(handler) or handler is None
        with self.__lock:
            self.__ref_count_free_handler = handler


@frozen
class StageReference:
    name: str


class CommitStageTask(Task):
    def __init__(self, stage: Stage, flow: Flow):
        super().__init__(
            fn=self.fn,
            name=f"Commit '{stage.name}'",
        )

        self.stage = stage
        self.flow = flow
        self.upstream_stages = {stage}

        self._bound_args = self._signature.bind()

    def fn(self):
        print(f"Committing Stage '{self.stage.name}'")
        # self.logger.info("Committing stage")
        # pydiverse.pipedag.config.store.commit_stage(self.stage)


class StageX:
    """The Stage class is used group a collection of related tasks

    The main purpose of a Stage is to allow for a transactionality mechanism.
    Only if all tasks inside a stage finish successfully does the stage
    get committed.

    All materialising task that get defined inside the stage's context
    will automatically get added to the stage.

    An example of how to use a Stage:
    ::

        @materialise()
        def my_materialising_task():
            return Table(...)

        with Flow("my_flow") as flow:
            with Stage("stage_1") as stage_1:
                task_1 = my_materialising_task()
                task_2 = another_materialising_task(task_1)

            with Stage("stage_2") as stage_2:
                task_3 = yet_another_task(task_3)
                ...

        flow.run()

    To ensure that all tasks get executed in the correct order, each
    MaterialisingTask must be an upstream dependency of its stage's
    StageCommitTask (this is done by calling the `add_task` method) and
    for each of its upstream dependencies, the associated StageCommitTask
    must be added as an upstream dependency.
    This ensures that the stage commit only happens after all tasks have
    finished writing to the transaction stage, and a task never gets executed
    before any of its upstream stage dependencies have been committed.

    :param name: The name of the stage. Two stages with the same name may
        not be used inside the same flow.


    Implementation
    --------------

    Reference Counting:

    PipeDAG uses a reference counting mechanism to keep track of how many
    tasks still depend on a specific stage for its inputs. Only once this
    reference counter hits 0 can the stage be unlocked (the PipeDAGStore
    object gets notified of thanks to `_set_ref_count_free_handler`.
    To modify increase and decrease the reference counter the
    `_incr_ref_count` and `_decr_ref_count` methods are used respectively.

    For MaterialisingTasks, incrementing of the counter happens inside
    the stage's `__exit__` function, decrementing happens using the provided
    `stage_ref_counter_handler`.
    """

    def __init__(self, name: str):
        self._name = None

        self.name = name
        self.transaction_name = f"{name}__tmp"

        # Variables that should be accessed via a lock
        self.__lock = threading.Lock()
        self.__did_commit = False
        self.__ref_count = 0
        self.__ref_count_free_handler = None

        # Tasks
        from pydiverse.pipedag.materialise import core

        self.task = StageCommitTask(self)
        self.materialising_tasks: list[materialise.MaterialisingTask] = []

        # Make sure that  exists on database
        # This also ensures that this stage's name is unique
        pydiverse.pipedag.config.store.register_stage(self)

    @property
    def current_name(self) -> str:
        """The name of the stage where the data currently lives

        Before a swap this is the transaction name, after the swap it is
        the base name.
        """

        if self.did_commit:
            return self.name
        else:
            return self.transaction_name
