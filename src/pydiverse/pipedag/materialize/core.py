from __future__ import annotations

import functools
import inspect
from functools import partial
from typing import TYPE_CHECKING, Callable

from pydiverse.pipedag._typing import CallableT
from pydiverse.pipedag.context import ConfigContext, RunContext, TaskContext
from pydiverse.pipedag.core.task import Task
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.container import Blob, Table
from pydiverse.pipedag.materialize.util import compute_cache_key
from pydiverse.pipedag.util import deep_map

if TYPE_CHECKING:
    from pydiverse.pipedag.materialize.metadata import TaskMetadata


def materialize(
    fn: CallableT = None,
    *,
    name: str = None,
    input_type: type = None,
    version: str = None,
    cache: Callable = None,
    lazy: bool = False,
    nout: int = 1,
) -> CallableT | MaterializingTask:
    if fn is None:
        return partial(
            materialize,
            name=name,
            input_type=input_type,
            version=version,
            cache=cache,
            lazy=lazy,
            nout=nout,
        )

    return MaterializingTask(
        fn,
        name=name,
        input_type=input_type,
        version=version,
        cache=cache,
        lazy=lazy,
        nout=nout,
    )


class MaterializingTask(Task):
    """Task whose outputs get materialized

    All the values a materializing task returns get written to the appropriate
    storage backend. Additionally, all `Table` and `Blob` objects in the
    input will be replaced with their appropriate objects (loaded from the
    storage backend). This means that tables and blobs never move from one
    task to another directly, but go through the storage layer instead.

    Because of how caching is implemented, task inputs and outputs must all
    be 'materializable'. This means that they can only contain objects of
    the following types:
    `dict`, `list`, `tuple`,
    `int`, `float`, `str`, `bool`, `None`,
    and PipeDAG's `Table` and `Blob` type.

    Automatically adds itself to the active stage.
    All materializing tasks MUST be defined inside a stage.

    :param fn: The run method of this task
    :key name: The name of this task
    :key input_type: The data type to convert table objects to when passed
        to this task.
    :key version: The version of this task. Unless this task is lazy, you
        always have to bump / change the version number to ensure that
        the new implementation gets used. Else a cached result might be used
        instead.
    :key cache: An explicit function for validating cache validity. If the output
        of this function changes while the source parameters are the same (e.g.
        the source is a filepath and `cache` loads data from this file), then the
        cache will be deemed invalid and is not used.
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
        input_type: type = None,
        version: str = None,
        cache: Callable = None,
        lazy: bool = False,
        nout: int = 1,
    ):
        super().__init__(
            MaterializationWrapper(fn),
            name=name,
            nout=nout,
        )

        self.input_type = input_type
        self.version = version
        self.cache = cache
        self.lazy = lazy

    # The following properties are all just convenient ways to access the state
    # stored in TaskContext. The reason these properties aren't stored inside the
    # task object itself is because tasks shouldn't have state, because they get
    # reused between different flow runs.

    @property
    def input_hash(self) -> str:
        return TaskContext.get().input_hash

    @input_hash.setter
    def input_hash(self, value):
        TaskContext.get().input_hash = value

    @property
    def cache_fn_hash(self) -> str | None:
        return TaskContext.get().cache_fn_hash

    @cache_fn_hash.setter
    def cache_fn_hash(self, value):
        TaskContext.get().cache_fn_hash = value

    @property
    def cache_metadata(self) -> TaskMetadata:
        return TaskContext.get().cache_metadata

    @cache_metadata.setter
    def cache_metadata(self, value):
        TaskContext.get().cache_metadata = value

    def combined_cache_key(self, from_cache_metadata=False):
        from pydiverse.pipedag.materialize.util.cache import compute_cache_key

        if from_cache_metadata and (cache_metadata := self.cache_metadata) is not None:
            name = cache_metadata.name
            version = cache_metadata.version
            input_hash = cache_metadata.input_hash
            cache_fn_hash = cache_metadata.cache_fn_hash
        else:
            name = self.name
            version = self.version
            input_hash = self.input_hash
            cache_fn_hash = self.cache_fn_hash

        return compute_cache_key(
            "TASK",
            name,
            version,
            input_hash,
            cache_fn_hash,
        )


class MaterializationWrapper:
    """Function wrapper that contains all high level materialization logic

    :param fn: The function to wrap
    """

    def __init__(self, fn: Callable):
        functools.update_wrapper(self, fn)

        self.fn = fn
        self.fn_signature = inspect.signature(fn)

    def __call__(self, *args, **kwargs):
        """Function wrapper / materialization logic

        :param args: The arguments passed to the function
        :param _pipedag_task_: The `MaterializingTask` instance which called
            this wrapper.
        :param kwargs: The keyword arguments passed to the function
        :return: A copy of what the original function returns annotated
            with some additional metadata.
        """

        task: MaterializingTask = TaskContext.get().task  # type: ignore
        store = ConfigContext.get().store
        bound = self.fn_signature.bind(*args, **kwargs)

        if task is None:
            raise TypeError("Task can't be None.")

        # If this is the first task in this stage to be executed, ensure that
        # the stage has been initialized and locked.
        store.ensure_stage_is_ready(task.stage)

        # Compute the cache key for the task inputs
        input_json = store.json_encode(bound.arguments)
        input_hash = compute_cache_key("INPUT", input_json)

        cache_fn_hash = ""
        if task.cache is not None:
            cache_fn_output = store.json_encode(task.cache(*args, **kwargs))
            cache_fn_hash = compute_cache_key("CACHE_FN", cache_fn_output)

        task.input_hash = input_hash
        task.cache_fn_hash = cache_fn_hash
        combined_cache_key = task.combined_cache_key()

        # Check if this task has already been run with the same inputs
        # If yes, return memoized result. This prevents DuplicateNameExceptions
        ctx = RunContext.get()
        with ctx.task_memo(task, combined_cache_key) as (success, memo):
            if success:
                task.logger.info(
                    "Task has already been run with the same inputs."
                    " Using memoized results."
                )

                return memo

            # Check the cache
            try:
                cached_output, cache_metadata = store.retrieve_cached_output(task)
                task.cache_metadata = cache_metadata
                if not task.lazy:
                    # Task isn't lazy -> copy cache to transaction stage
                    store.copy_cached_output_to_transaction_stage(
                        cached_output, cache_metadata, task
                    )
                    ctx.store_task_memo(task, combined_cache_key, cached_output)
                    task.logger.info("Found task in cache. Using cached result.")
                    return cached_output
            except CacheError as e:
                task.logger.info(
                    "Failed to retrieve task from cache.",
                    exception=str(e),
                )

            # Not found in cache / lazy -> Evaluate Function
            args, kwargs = store.dematerialize_task_inputs(
                task, bound.args, bound.kwargs
            )

            result = self.fn(*args, **kwargs)
            result = store.materialize_task(task, result)

            # Delete underlying objects from result (after materializing them)
            def obj_del_mutator(x):
                if isinstance(x, (Table, Blob)):
                    x.obj = None
                return x

            result = deep_map(result, obj_del_mutator)
            ctx.store_task_memo(task, combined_cache_key, result)
            self.value = result

            return result
