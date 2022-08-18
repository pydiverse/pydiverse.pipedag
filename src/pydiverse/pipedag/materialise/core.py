from __future__ import annotations

import functools
import inspect
from functools import partial
from typing import Callable

from pydiverse.pipedag._typing import CallableT
from pydiverse.pipedag.context import ConfigContext, RunContext, TaskContext
from pydiverse.pipedag.core.task import Task
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialise.container import Blob, Table
from pydiverse.pipedag.util import deepmutate


def materialise(
    fn: CallableT = None,
    *,
    name: str = None,
    input_type: type = None,
    version: str = None,
    lazy: bool = False,
    nout: int = 1,
) -> CallableT | MaterialisingTask:
    if fn is None:
        return partial(
            materialise,
            name=name,
            input_type=input_type,
            version=version,
            lazy=lazy,
            nout=nout,
        )

    return MaterialisingTask(
        fn,
        name=name,
        input_type=input_type,
        version=version,
        lazy=lazy,
        nout=nout,
    )


class MaterialisingTask(Task):
    """Task whose outputs get materialised

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

    Automatically adds itself to the active stage.
    All materialising tasks MUST be defined inside a stage.

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
        input_type: type = None,
        version: str = None,
        lazy: bool = False,
        nout: int = 1,
    ):
        super().__init__(
            MaterialisationWrapper(fn),
            name=name,
            nout=nout,
            pass_task=True,
        )

        self.input_type = input_type
        self.version = version
        self.lazy = lazy

        self.cache_key = None

    def prepare_for_run(self):
        super().prepare_for_run()
        self.cache_key = None


class MaterialisationWrapper:
    """Function wrapper that contains all high level materialisation logic

    :param fn: The function to wrap
    """

    def __init__(self, fn: Callable):
        functools.update_wrapper(self, fn)

        self.fn = fn
        self.fn_signature = inspect.signature(fn)

    def __call__(self, *args, **kwargs):
        """Function wrapper / materialisation logic

        :param args: The arguments passed to the function
        :param _pipedag_task_: The `MaterialisingTask` instance which called
            this wrapper.
        :param kwargs: The keyword arguments passed to the function
        :return: A copy of what the original function returns annotated
            with some additional metadata.
        """

        task = TaskContext.get().task
        store = ConfigContext.get().store
        bound = self.fn_signature.bind(*args, **kwargs)

        if task is None:
            raise TypeError("Task can't be None.")

        # If this is the first task in this stage to be executed, ensure that
        # the stage has been initialized and locked.
        store.ensure_stage_is_ready(task.stage)

        # Compute the cache key for the task inputs
        input_json = store.json_encode(bound.arguments)
        cache_key = store.compute_task_cache_key(task, input_json)
        task.cache_key = cache_key

        # task.logger.info(
        #     "Task is currently being run with the same inputs."
        #     " Waiting for the other task to finish..."
        # )

        # Check if this task has already been run with the same inputs
        # If yes, return memoized result. This prevents DuplicateNameExceptions
        ctx = RunContext.get()
        with ctx.task_memo(task, cache_key) as (success, memo):
            if success:
                task.logger.info(
                    "Task has already been run with the same inputs."
                    " Using memoized results."
                )

                return memo

            # If task is not lazy, check the cache
            if not task.lazy:
                try:
                    cached_output = store.retrieve_cached_output(task)
                    store.copy_cached_output_to_transaction_stage(cached_output, task)
                    ctx.store_task_memo(task, cache_key, cached_output)
                    task.logger.info(f"Found task in cache. Using cached result.")
                    return cached_output
                except CacheError as e:
                    task.logger.info(f"Failed to retrieve task from cache. {e}")
                    pass

            # Not found in cache / lazy -> Evaluate Function
            args, kwargs = store.dematerialise_task_inputs(
                task, bound.args, bound.kwargs
            )

            result = self.fn(*args, **kwargs)
            result = store.materialise_task(task, result)

            # Delete underlying objects from result (after materialising them)
            def obj_del_mutator(x):
                if isinstance(x, (Table, Blob)):
                    x.obj = None
                return x

            result = deepmutate(result, obj_del_mutator)
            ctx.store_task_memo(task, cache_key, result)
            self.value = result

            return result
