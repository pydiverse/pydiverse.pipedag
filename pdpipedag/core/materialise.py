from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Type

import prefect

import pdpipedag
from pdpipedag.core.schema import schema_ref_counter_handler
from pdpipedag.errors import FlowError, CacheError
from pdpipedag._typing import CallableT


def materialise(**kwargs):
    """Decorator to create MaterialisingTasks from functions

    For a list of arguments, check out the `MaterialisingTask` documentation.

    Usage example:
    ::
        @materialise(input_type = pd.DataFrame, version = "1.0")
        def multiply_df(df: pd.DataFrame, by: float):
            return Table(df * by)

    """
    def wrapper(fn: CallableT) -> CallableT:
        return MaterialisingTask(fn, **kwargs)
    return wrapper


class MaterialisingTask(prefect.Task):
    """ Task whose outputs get materialised

    All the values a materialising task returns get written to the appropriate
    storage backend. Additionally, all `Table` and `Blob` objects in the
    input will be replaced with their appropriate objects (loaded from the
    storage backend). This means that tables and blobs never move from one
    task to another directly, but go through the storage layer instead.

    Because of how caching is implemented, task inputs and outputs must all
    be 'materialisable'. This means that they can only contain objects of
    the following types:
    `dict`, `list`, `tuple`,
    `int`, `float`, `str`, `bool`, `None`,
    and PipeDAG's `Table` and `Blob` type.

    Automatically adds itself to the active schema for schema swapping.
    All materialising tasks MUST be defined inside a schema.

    :param fn: The run method of this task
    :key name: The name of this task
    :key input_type: The data type to convert table objects to when passed
        to this task.
    :key version: The version of this task. Unless this task is lazy, you
        always have to bump / change the version number to ensure that
        the new implementation gets used. Else a cached result might be used
        instead.
    :key lazy: Boolean indicating if this task should be lazy. A lazy task is
        a task that always gets executed, and if it produces a lazy table
        (e.g. a SQL query), the backend can compare the generated output
        to see it the same query has been executed before (and only execute
        it if not). This is an alternative to manually setting the version
        number.
    :key kwargs: Any other keyword arguments will directly get passed to the
        prefect Task initializer.
    """

    def __init__(
            self,
            fn: Callable,
            *,
            name: str = None,
            input_type: Type = None,
            version: str = None,
            lazy: bool = False,
            **kwargs: Any
    ):
        if not callable(fn):
            raise TypeError("`fn` must be callable")

        # Set the Prefect name from the function
        if name is None:
            name = getattr(fn, "__name__", type(self).__name__)
        self.original_name = name

        # Run / Signature Handling
        prefect.core.task._validate_run_signature(fn)

        self.run = lambda: self.run()
        self.wrapped_fn = MaterialisationWrapper(fn)

        functools.update_wrapper(self.run, fn)
        functools.update_wrapper(self, fn)

        super().__init__(name=name, **kwargs)

        self.input_type = input_type
        self.version = version
        self.lazy = lazy

        self.schema = None
        self.upstream_schemas = None
        self.cache_key = None

        self.state_handlers.append(schema_ref_counter_handler)

    def run(self) -> None:
        # This is just a stub.
        # As soon as this object called, this run method gets replaced with
        # the actual implementation.
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        new = super().__call__(*args, **kwargs)  # type: MaterialisingTask
        new._update_new_task()
        return new

    def map(self, *args, **kwargs):
        new = super().map(*args, **kwargs)  # type: MaterialisingTask
        new._update_new_task()
        return new

    def _update_new_task(self):
        """
        Both `__call__` and `map` create new instances of the Task. This
        method is used to modify those copies, add relevant metadata, and
        add them to the schema in which they were created.
        """

        self.schema = prefect.context.get('pipedag_schema')
        self.name = f"{self.original_name}({self.schema.name})"
        if self.schema is None:
            raise FlowError(
                "Schema missing for materialised task. Materialised tasks must "
                "be used inside a schema block.")

        # Create run method
        self.run = (lambda *args, **kwargs: self.wrapped_fn(
            *args,
            **kwargs,
            _pipedag_task_ = self
        ))
        functools.update_wrapper(self.run, self.wrapped_fn)

        # Add task to schema
        self.schema.add_task(self)

    def _incr_schema_ref_count(self, by: int = 1):
        """
        Private method required for schema reference counting:
        `pdpipedag.core.schema.schema_ref_counter_handler`

        Increments the reference count to the schema in which this task was
        defined, and all schemas that appear in its inputs.
        """
        self.schema._incr_ref_count(by)
        for upstream_schema in self.upstream_schemas:
            upstream_schema._incr_ref_count(by)

    def _decr_schema_ref_count(self, by: int = 1):
        """
        Private method required for schema reference counting:
        `pdpipedag.core.schema.schema_ref_counter_handler`

        Decrements the reference count to the schema in which this task was
        defined, and all schemas that appear in its inputs.
        """
        self.schema._decr_ref_count(by)
        for upstream_schema in self.upstream_schemas:
            upstream_schema._decr_ref_count(by)


class MaterialisationWrapper:
    """Function wrapper that contains all high level materialisation logic

    :param fn: The function to wrap
    """

    def __init__(self, fn: Callable):
        self.fn = fn
        self.fn_signature = inspect.signature(fn)

    def __call__(self, *args, _pipedag_task_: MaterialisingTask, **kwargs):
        """Function wrapper / materialisation logic

        :param args: The arguments passed to the function
        :param _pipedag_task_: The `MaterialisingTask` instance which called
            this wrapper.
        :param kwargs: The keyword arguments passed to the function
        :return: A copy of what the original function returns annotated
            with some additional metadata.
        """
        task = _pipedag_task_
        store = pdpipedag.config.store
        bound = self.fn_signature.bind(*args, **kwargs)

        # If this is the first task in this schema to be executed, ensure that
        # the schema has been created and locked.
        store.ensure_schema_is_ready(task.schema)

        # Compute the cache key for the task inputs
        input_json = store.json_encode(bound.arguments)
        cache_key = store.compute_task_cache_key(task, input_json)
        task.cache_key = cache_key

        if not task.lazy:
            # Try to retrieve output from cache
            try:
                cached_output = store.retrieve_cached_output(task)
                store.copy_cached_output_to_working_schema(cached_output, task)
                task.logger.info(f"Found task in cache. Using cached result.")
                return cached_output
            except CacheError as e:
                task.logger.info(f"Failed to retrieve task from cache. {e}")
                pass

        # Not found in cache / lazy -> Evaluate Function
        args, kwargs = store.dematerialise_task_inputs(task, bound.args, bound.kwargs)
        result = self.fn(*args, **kwargs)

        # Materialise
        materialised_result = store.materialise_task(task, result)
        return materialised_result
