import functools
import inspect
from typing import Callable, Any, TypeVar, Type

import prefect

import pdpipedag
from pdpipedag._typing import T
from pdpipedag.errors import FlowError, CacheError


def materialise(**kwargs):
    CallableT = TypeVar('CallableT', bound = Callable)
    def wrapper(fn: CallableT) -> CallableT:
        return MaterialisingTask(fn, **kwargs)
    return wrapper


class MaterialisingTask(prefect.Task):
    """
    Task that gets materialised. Automatically adds itself to the active
    schema for schema swapping.
    """

    def __init__(
            self,
            fn: Callable,
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


class MaterialisationWrapper:

    def __init__(self, fn: Callable):
        self.fn = fn
        self.fn_signature = inspect.signature(fn)

    def __call__(self, *args, _pipedag_task_: MaterialisingTask, **kwargs):
        task = _pipedag_task_
        store = pdpipedag.config.store
        bound = self.fn_signature.bind(*args, **kwargs)

        print(task.name, id(task), prefect.context.get("task_slug"))

        # Compute the cache key for the task inputs
        input_json = store.json_serialise(bound.arguments)
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
