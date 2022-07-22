from __future__ import annotations

import contextlib
import threading
from typing import Iterator, Callable

import prefect

import pdpipedag
from pdpipedag.core import materialise
from pdpipedag.errors import SchemaError


class Schema:
    """The Schema class is used group a collection of related tasks

    The main purpose of a Schema is to enable 'schema swapping': a technique
    where you materialize all task outputs into a temporary empty schema
    (called the 'working schema') and only once all tasks have finished
    running *successfully* you swap the 'base schema' with the 'working
    schema' (usually done by renaming them).

    All materialising task that get defined inside the schema's context
    will automatically get added to the schema.

    An example of how to use a Schema:
    ::

        @materialise()
        def my_materialising_task():
            return Table(...)

        with Flow("my_flow") as flow:
            with Schema("my_first_schema") as schema_1:
                task_1 = my_materialising_task()
                task_2 = another_materialising_task(task_1)

            with Schema("another_schema") as schema_2:
                task_3 = yet_another_task(task_3)
                ...

        flow.run()

    To ensure that all tasks get executed in the correct order, each
    MaterialisingTask must be an upstream dependency of its schema's
    SchemaSwapTask (this is done by calling the `add_task` method) and
    for each of its upstream dependencies, the associated SchemaSwapTask
    must be added as an upstream dependency.
    This ensures that the schema swap only happens after all tasks have
    finished writing to the working schema, and a task never gets executed
    before any of its upstream schema dependencies have been swapped.


    :param name: The name of the schema. Two schemas with the same name may
        not be used inside the same flow


    Implementation
    --------------

    Reference Counting:

    PipeDAG uses a reference counting mechanism to keep track of how many
    tasks still depend on a specific schema for its inputs. Only once this
    reference counter hits 0 can the schema be unlocked (the PipeDAGStore
    object gets notified of thanks to `_set_ref_count_free_handler`.
    To modify increase and decrease the reference counter the
    `_incr_ref_count` and `_decr_ref_count` methods are used respectively.

    For MaterialisingTasks, incrementing of the counter happens inside
    the schemas `__exit__` function, decrementing happens using the provided
    `schema_ref_counter_handler`.
    """

    def __init__(self, name: str):
        self.name = name
        self.working_name = f"{name}__pipedag"

        # Variables that should be accessed via a lock
        self.__lock = threading.Lock()
        self.__did_swap = False
        self.__ref_count = 0
        self.__ref_count_free_handler = None

        # Tasks
        self.task = SchemaSwapTask(self)
        self.materialising_tasks: list[materialise.MaterialisingTask] = []

        # Make sure that schema exists on database
        # This also ensures that this schema name is unique
        pdpipedag.config.store.register_schema(self)

    def __repr__(self):
        return f"<Schema: {self.name}>"

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, Schema):
            return False
        return self.name == other.name

    def __enter__(self):
        """Schema context manager - enter

        Adds the current schema to the prefect context as 'pipedag_schema'
        and adds the schema swap task to the flow.

        All MaterialisingTasks that get defined in this context must call
        this schema's `add_task` method.
        """
        self.flow: prefect.Flow = prefect.context.flow

        # Store current schema in context
        self._enter_schema = prefect.context.get("pipedag_schema")
        prefect.context.pipedag_schema = self

        # Add schema task to flow
        self.flow.add_task(self.task)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Schema context manager - exit

        Creates missing up- and downstream dependencies for SchemaSwapTasks.
        """

        upstream_edges = self.flow.all_upstream_edges()
        downstream_edges = self.flow.all_downstream_edges()

        def get_upstream_schemas(task: prefect.Task) -> Iterator["Schema"]:
            """Get all direct schema dependencies of a task

            The direct schema dependencies is the set of all schemas
            (excluding the schema of the task itself) that get visited
            by a DFS on the upstream tasks that stops whenever it
            encounters a materialising task.
            In other words: it is the set of all upstream schemas that can
            be reached without going through another materialising task.

            This recursive implementation seems to be better than a
            classic DFS that also keeps track of which nodes it has already
            visited. But because (almost) all inputs of a task are also
            materialising tasks, this function should never recurse deep.

            :param task: The task for which to get the upstream schemas
            :return: Yields all upstream schemas
            """
            for up_task in [edge.upstream_task for edge in upstream_edges[task]]:
                if isinstance(up_task, materialise.MaterialisingTask):
                    if up_task.schema is not self:
                        yield up_task.schema
                    continue
                yield from get_upstream_schemas(up_task)

        def needs_downstream(task: prefect.Task) -> bool:
            """Determine if a task needs the downstream schema swap dependency

            Only tasks that don't have other tasks of the same schema as
            downstream dependencies need to add the schema swap task as
            a downstream dependency.
            """
            for edge in downstream_edges[task]:
                down_task = edge.downstream_task
                if isinstance(down_task, materialise.MaterialisingTask):
                    if down_task.schema is self:
                        return False
                    if not needs_downstream(down_task):
                        return False
            return True

        # For each task, add the appropriate upstream schema swap dependencies
        for task in self.materialising_tasks:
            upstream_schemas = list(get_upstream_schemas(task))
            task.upstream_schemas = upstream_schemas
            for dependency in upstream_schemas:
                task.set_upstream(dependency.task)

            # Must be done here, because _incr_schema_ref_count depends
            # on the list of upstream schemas
            task._incr_schema_ref_count()

        # Add downstream schema swap dependency
        for task in self.materialising_tasks:
            if needs_downstream(task):
                task.set_downstream(self.task)

        # Restore context
        prefect.context.pipedag_schema = self._enter_schema

    def add_task(self, task: materialise.MaterialisingTask):
        """Add a MaterialisingTask to the Schema"""
        assert isinstance(task, materialise.MaterialisingTask)
        self.materialising_tasks.append(task)

    @property
    def current_name(self) -> str:
        """The name of the schema where the data currently lives

        Before a swap this is the working name, after the swap it is
        the base name.
        """

        if self.did_swap:
            return self.name
        else:
            return self.working_name

    @property
    def did_swap(self) -> bool:
        with self.__lock:
            return self.__did_swap

    @contextlib.contextmanager
    def perform_swap(self):
        """Context manager to update the `did_swap` property

        When the backend performs a schema swap, this must be done inside
        a with statement using this context manager. This ensures that
        a schema never gets swapped twice (in case of a race condition).
        """
        with self.__lock:
            if self.__did_swap:
                raise SchemaError(f"Schema {self.name} has already been swapped.")

            try:
                yield self
            finally:
                self.__did_swap = True

    def _incr_ref_count(self, by: int = 1):
        with self.__lock:
            self.__ref_count += by

    def _decr_ref_count(self, by: int = 1):
        with self.__lock:
            self.__ref_count -= by
            assert self.__ref_count >= 0

            if self.__ref_count == 0 and callable(self.__ref_count_free_handler):
                self.__ref_count_free_handler(self)

    def _set_ref_count_free_handler(self, handler: Callable[[Schema], None]):
        assert callable(handler) or handler is None
        with self.__lock:
            self.__ref_count_free_handler = handler


class SchemaSwapTask(prefect.Task):
    """Swaps schema once all materialising task have finished successfully"""

    def __init__(self, schema):
        super().__init__(name=f"SchemaSwapTask({schema.name})")
        self.schema = schema

        self._incr_schema_ref_count()
        self.state_handlers.append(schema_ref_counter_handler)

    def run(self):
        self.logger.info("Performing schema swap.")
        pdpipedag.config.store.swap_schema(self.schema)

    def _incr_schema_ref_count(self, by: int = 1):
        self.schema._incr_ref_count(by)

    def _decr_schema_ref_count(self, by: int = 1):
        self.schema._decr_ref_count(by)


def schema_ref_counter_handler(task, old_state, new_state):
    """Prefect Task state handler to update the schema reference counter

    For internal use only;
    Decreases the reference counters of all schemas associated with a task
    by one once the task has finished running.

    Requires the task to implement the following methods:
    ::
        task._incr_schema_ref_count(by: int = 1)
        task._decr_schema_ref_count(by: int = 1)
    """

    if isinstance(new_state, prefect.engine.state.Mapped):
        # Is mapping task -> Increment reference counter by the number
        # of child tasks.
        task._incr_schema_ref_count(new_state.n_map_states)

    if isinstance(new_state, prefect.engine.state.Failed):
        run_count = prefect.context.get("task_run_count", 0)
        if run_count <= task.max_retries:
            # Will retry -> Don't decrement ref counter
            return

    if isinstance(new_state, prefect.engine.state.Finished):
        # Did finish task and won't retry -> Decrement ref counter
        task._decr_schema_ref_count()
