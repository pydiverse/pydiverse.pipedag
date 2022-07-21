from __future__ import annotations

import contextlib
import threading
from typing import Iterator, Callable

import prefect

import pdpipedag
from pdpipedag.core import materialise
from pdpipedag.errors import SchemaError


class Schema:

    def __init__(self, name: str):
        self.name = name
        self.working_name = f'{name}__pipedag'

        # Variables that should be accessed via a lock
        self.__lock = threading.Lock()
        self.__did_swap = False
        self.__ref_count = 0
        self.__ref_count_free_handler = None

        # Tasks
        self.task = SchemaSwapTask(self)
        self.materialising_tasks = []

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
        self.flow: prefect.Flow = prefect.context.flow

        # Store current schema in context
        self._enter_schema = prefect.context.get('pipedag_schema')
        prefect.context.pipedag_schema = self

        # Add schema task to flow
        self.flow.add_task(self.task)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # The MaterialisingTask wrapper already adds the schema as a
        # downstream dependency. The only thing left to do is adding
        # the upstream dependencies to the upstream swap schema tasks.

        upstream_edges = self.flow.all_upstream_edges()

        def get_upstream_schemas(task: prefect.Task) -> Iterator['Schema']:
            """ Perform DFS and get all upstream schema dependencies.

            :param task: The task for which to get the upstream schemas.
            :return: Yields all upstream schemas.
            """
            visited = set()
            stack = [task]

            while stack:
                top = stack.pop()
                if top in visited:
                    continue
                visited.add(top)

                if isinstance(top, materialise.MaterialisingTask):
                    if top.schema != self:
                        yield top.schema
                        continue

                for edge in upstream_edges[top]:
                    stack.append(edge.upstream_task)

        # For each task, add the appropriate upstream schema swap dependencies
        for task in self.materialising_tasks:
            upstream_schemas = list(get_upstream_schemas(task))
            task.upstream_schemas = upstream_schemas
            for dependency in upstream_schemas:
                task.set_upstream(dependency.task)

            task._incr_schema_ref_count()

        # Restore context
        prefect.context.pipedag_schema = self._enter_schema

    def add_task(self, task: materialise.MaterialisingTask):
        assert isinstance(task, materialise.MaterialisingTask)
        task.set_downstream(self.task)
        self.materialising_tasks.append(task)

    @property
    def current_name(self) -> str:
        """ The name of the schema where the data currently lives.

        Before a swap this is the working schema name and after a swap it is
        the actual schema name.
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

    def __init__(self, schema):
        super().__init__(name = f'SchemaSwapTask({schema.name})')
        self.schema = schema

        self._incr_schema_ref_count()
        self.state_handlers.append(schema_ref_counter_handler)

    def run(self):
        self.logger.info('Performing schema swap.')
        pdpipedag.config.store.swap_schema(self.schema)

    def _incr_schema_ref_count(self, by: int = 1):
        self.schema._incr_ref_count(by)

    def _decr_schema_ref_count(self, by: int = 1):
        self.schema._decr_ref_count(by)


def schema_ref_counter_handler(task, old_state, new_state):
    """Prefect Task state handler to update the schema reference counter

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
        run_count = prefect.context.get('task_run_count', 0)
        if run_count <= task.max_retries:
            # Will retry -> Don't decrement ref counter
            return

    if isinstance(new_state, prefect.engine.state.Finished):
        # Did finish task and won't retry -> Decrement ref counter
        task._decr_schema_ref_count()
