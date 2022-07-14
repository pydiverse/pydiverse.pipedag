import functools
import inspect
from typing import Callable, Any, TypeVar, Type

import prefect

import pdpipedag
from pdpipedag._typing import T
from pdpipedag.core import table as pdtbl
from pdpipedag.core.util.deepmutate import deepmutate
from pdpipedag.errors import FlowError


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
            raise FlowError("Schema missing for materialised task. Materialised tasks must be used inside a schema block.")

        # Create run method
        new.run = (lambda *args, **kwargs: new.wrapped_fn(
            *args,
            **kwargs,
            _pipedag_schema=new.schema,
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

    def __call__(self, *args, _pipedag_schema, **kwargs):
        # Must pass schema to __call__ because the same wrapper instance
        # gets used my multiple instances of MaterialisingTask.
        schema = _pipedag_schema

        # Run the function
        args, kwargs = self.load_arguments(args, kwargs)
        result = self.fn(*args, **kwargs)

        # Materialise the results back to our database
        # + assign schema to return values
        materialised_result = self.materialise_values(result, schema)
        return materialised_result

    #### Arguments Loading ####

    def load_arguments(self, args, kwargs) -> tuple[tuple, dict]:
        bound_signature = self.fn_signature.bind(*args, **kwargs)
        for name, value in bound_signature.arguments.items():
            bound_signature.arguments[name] = deepmutate(value, self._load_mutator)
        return bound_signature.args, bound_signature.kwargs

    def _load_mutator(self, x):
        if isinstance(x, pdtbl.Table):
            return pdpipedag.config.table_backend.retrieve_table_obj(x, as_type = self.input_type)
        return x

    #### Result Materialisation ####

    def materialise_values(self, value, schema):
        def _materialise_mutator(x):
            if isinstance(x, pdtbl.Table):
                x.schema = schema
                pdpipedag.config.table_backend.store_table(x)
            return x

        return deepmutate(value, _materialise_mutator)
