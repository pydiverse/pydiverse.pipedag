import functools
import inspect
from typing import Callable, Any, TypeVar, Type

import prefect

import pdpipedag
from pdpipedag._typing import T
from pdpipedag.errors import FlowError, CacheError


def materialise(
        *,
        name: str = None,
        type: Type = None,
        **kwargs,
):

    CallableT = TypeVar('CallableT', bound = Callable)
    def wrapper(fn: CallableT) -> CallableT:
        return MaterialisingTask(
            fn,
            name = name,
            input_type = type,
            **kwargs
        )

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
            **kwargs: Any
    ):
        if not callable(fn):
            raise TypeError("`fn` must be callable")

        # Set the Prefect name from the function
        if name is None:
            name = getattr(fn, "__name__", type(self).__name__)

        # Run / Signature Handling
        prefect.core.task._validate_run_signature(fn)

        self.run = lambda: self.run()
        self.wrapped_fn = MaterialisationWrapper(
            fn,
            input_type = input_type,
        )

        functools.update_wrapper(self.wrapped_fn, fn)
        functools.update_wrapper(self.run, fn)
        functools.update_wrapper(self, fn)

        super().__init__(name=name, **kwargs)

        self.input_type = input_type

        self.schema = None
        self.upstream_schemas = None

    def run(self) -> None:
        # This is just a stub.
        # As soon as this object called, this run method gets replaced with
        # the actual implementation.
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        new = super().__call__(*args, **kwargs)  # type: MaterialisingTask

        new.schema = prefect.context.get('pipedag_schema')
        new.name = f"{new.name}({new.schema.name})"
        if new.schema is None:
            raise FlowError(
                "Schema missing for materialised task. Materialised tasks must "
                "be used inside a schema block.")

        # Create run method
        new.run = (lambda *args, **kwargs: new.wrapped_fn(
            *args,
            **kwargs,
            _pipedag_ = {
                'task': new,
                'schema': new.schema,
            }
        ))
        functools.update_wrapper(new.run, new.wrapped_fn)

        # Add task to schema
        new.schema.add_task(new)
        return new


class MaterialisationWrapper:

    def __init__(
            self,
            fn: Callable,
            input_type: Type[T],
    ):
        self.fn = fn
        self.fn_signature = inspect.signature(fn)
        self.input_type = input_type

    def __call__(self, *args, _pipedag_, **kwargs):
        task = _pipedag_['task']
        store = pdpipedag.config.store
        bound = self.fn_signature.bind(*args, **kwargs)

        # Try to retrieve output from cache
        input_json = store.json_serialise(bound.arguments)
        cache_key = store.compute_cache_key(task, input_json)

        try:
            cached_output = store.retrieve_cached_output(task, cache_key)
            store.copy_cached_output_to_working_schema(cached_output)
            task.logger.info(f"Found task in cache. Using cached result.")
            return cached_output
        except CacheError as e:
            task.logger.info(f"Failed to retrieve task from cache. {e}")
            pass

        # Not found in cache -> Evaluate Function
        args, kwargs = store.dematerialise_task_inputs(task, bound.args, bound.kwargs)
        result = self.fn(*args, **kwargs)

        # Materialise
        materialised_result = store.materialise_task(task, cache_key, result)
        return materialised_result
