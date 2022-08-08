from __future__ import annotations

import contextlib
import threading
from typing import Callable, Iterator

import prefect

import pydiverse.pipedag
from pydiverse.pipedag.core import materialise
from pydiverse.pipedag.errors import StageError
from pydiverse.pipedag.util import normalise_name


class Stage:
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
        self.task = StageCommitTask(self)
        self.materialising_tasks: list[materialise.MaterialisingTask] = []

        # Make sure that  exists on database
        # This also ensures that this stage's name is unique
        pydiverse.pipedag.config.store.register_stage(self)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = normalise_name(value)

    def __repr__(self):
        return f"<Stage: {self.name}>"

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, Stage):
            return False
        return self.name == other.name

    def __enter__(self) -> Stage:
        """Stage context manager - enter

        Adds the current stage to the prefect context as 'pipedag_stage'
        and adds the stage commit task to the flow.

        All MaterialisingTasks that get defined in this context must call
        this stage's `add_task` method.
        """
        self.flow: prefect.Flow = prefect.context.flow

        # Store current stage in context
        self._enter_stage = prefect.context.get("pipedag_stage")
        prefect.context.pipedag_stage = self

        # Add stage commit task to flow
        self.flow.add_task(self.task)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stage context manager - exit

        Creates missing up- and downstream dependencies for StageCommitTask.
        """

        upstream_edges = self.flow.all_upstream_edges()
        downstream_edges = self.flow.all_downstream_edges()

        def get_upstream_stages(task: prefect.Task) -> Iterator[Stage]:
            """Get all direct stage dependencies of a task

            The direct stage dependencies is the set of all stages
            (excluding the stage of the task itself) that get visited
            by a DFS on the upstream tasks that stops whenever it
            encounters a materialising task.
            In other words: it is the set of all upstream stages that can
            be reached without going through another materialising task.

            This recursive implementation seems to be better than a
            classic DFS that also keeps track of which nodes it has already
            visited. But because (almost) all inputs of a task are also
            materialising tasks, this function should never recurse deep.

            :param task: The task for which to get the upstream stages
            :return: Yields all upstream stages
            """
            for up_task in [edge.upstream_task for edge in upstream_edges[task]]:
                if isinstance(up_task, materialise.MaterialisingTask):
                    if up_task.stage is not self:
                        yield up_task.stage
                    continue
                yield from get_upstream_stages(up_task)

        def needs_downstream(task: prefect.Task) -> bool:
            """Determine if a task needs the downstream stage commit dependency

            Only tasks that don't have other tasks of the same stage as
            downstream dependencies need to add the stage commit task as
            a downstream dependency.
            """
            for edge in downstream_edges[task]:
                down_task = edge.downstream_task
                if isinstance(down_task, materialise.MaterialisingTask):
                    if down_task.stage is self:
                        return False
                    if not needs_downstream(down_task):
                        return False
            return True

        # For each task, add the appropriate upstream stage commit dependencies
        for task in self.materialising_tasks:
            upstream_stages = list(get_upstream_stages(task))
            task.upstream_stages = upstream_stages
            for dependency in upstream_stages:
                task.set_upstream(dependency.task)

            # Must be done here, because _incr_stage_ref_count depends
            # on the list of upstream stages
            task._incr_stage_ref_count()

        # Add downstream stage commit dependency
        for task in self.materialising_tasks:
            if needs_downstream(task):
                task.set_downstream(self.task)

        # Restore context
        prefect.context.pipedag_stage = self._enter_stage

    def add_task(self, task: materialise.MaterialisingTask):
        """Add a MaterialisingTask to the Stage"""
        assert isinstance(task, materialise.MaterialisingTask)
        self.materialising_tasks.append(task)

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

    @property
    def did_commit(self) -> bool:
        with self.__lock:
            return self.__did_commit

    @contextlib.contextmanager
    def commit_context(self):
        """Context manager to update the `did_swap` property

        When the backend commits a stage, this must be done inside
        a with statement using this context manager. This ensures that
        a stage never gets committed twice (in case of a race condition).
        """
        with self.__lock:
            if self.__did_commit:
                raise StageError(f"Stage {self.name} has already been committed.")

            try:
                yield self
            finally:
                self.__did_commit = True

    @property
    def _ref_count(self):
        """The current reference counter value. For testing purposes only!"""
        with self.__lock:
            return self.__ref_count

    def _incr_ref_count(self, by: int = 1):
        with self.__lock:
            self.__ref_count += by

    def _decr_ref_count(self, by: int = 1):
        with self.__lock:
            self.__ref_count -= by
            assert self.__ref_count >= 0

            if self.__ref_count == 0 and callable(self.__ref_count_free_handler):
                self.__ref_count_free_handler(self)

    def _set_ref_count_free_handler(self, handler: Callable[[Stage], None]):
        assert callable(handler) or handler is None
        with self.__lock:
            self.__ref_count_free_handler = handler


class StageCommitTask(prefect.Task):
    """Commits a stage once all materialising task have finished successfully"""

    def __init__(self, stage: Stage):
        super().__init__(name=f"StageCommitTask({stage.name})")
        self.stage = stage

        self._incr_stage_ref_count()
        self.state_handlers.append(stage_ref_counter_handler)

    def run(self):
        self.logger.info("Committing stage")
        pydiverse.pipedag.config.store.commit_stage(self.stage)

    def _incr_stage_ref_count(self, by: int = 1):
        self.stage._incr_ref_count(by)

    def _decr_stage_ref_count(self, by: int = 1):
        self.stage._decr_ref_count(by)


def stage_ref_counter_handler(task, old_state, new_state):
    """Prefect Task state handler to update the stage's reference counter

    For internal use only;
    Decreases the reference counters of all stages associated with a task
    by one once the task has finished running.

    Requires the task to implement the following methods:
    ::
        task._incr_stage_ref_count(by: int = 1)
        task._decr_stage_ref_count(by: int = 1)
    """

    if new_state.is_mapped():
        # Is mapping task -> Increment reference counter by the number
        # of child tasks.
        task._incr_stage_ref_count(new_state.n_map_states)

    if new_state.is_failed():
        run_count = prefect.context.get("task_run_count", 0)
        if run_count <= task.max_retries:
            # Will retry -> Don't decrement ref counter
            return

    if new_state.is_finished():
        # Did finish task and won't retry -> Decrement ref counter
        task._decr_stage_ref_count()
