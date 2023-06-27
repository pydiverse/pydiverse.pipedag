from __future__ import annotations

import functools
import inspect
import warnings
from typing import TYPE_CHECKING, Callable, Protocol

from pydiverse.pipedag._typing import CallableT

if TYPE_CHECKING:

    class EngineDispatchedFn(Protocol):
        dialect: Callable[[str], Callable[[CallableT], CallableT]]
        original: CallableT


__all__ = [
    "engine_dispatch",
    "engine_argument_dispatch",
    "classmethod_engine_argument_dispatch",
]


def engine_dispatch(fn: CallableT) -> CallableT | EngineDispatchedFn:
    """Dispatch a function based on `self.engine.dialect.name`."""

    def get_dialect(*args, **kwargs):
        self_or_cls = args[0]
        return self_or_cls.engine.dialect

    return _engine_dispatch_generator(fn, get_dialect)


def engine_argument_dispatch(fn: CallableT) -> CallableT | EngineDispatchedFn:
    """Dispatch a function based on argument named `engine`"""
    signature = inspect.signature(fn)

    def get_dialect(*args, **kwargs):
        bound = signature.bind(*args, **kwargs)
        engine = bound.arguments["engine"]
        return engine.dialect

    return _engine_dispatch_generator(fn, get_dialect)


def classmethod_engine_argument_dispatch(
    fn: CallableT,
) -> CallableT | EngineDispatchedFn | classmethod:
    dispatched = engine_argument_dispatch(fn)
    cm_dispatched = classmethod(dispatched)
    cm_dispatched.dialect = dispatched.dialect
    cm_dispatched.original = fn
    return cm_dispatched


def _engine_dispatch_generator(fn, dialect_getter):
    fn_variants = {}

    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        dialect = dialect_getter(*args, **kwargs)
        dispatched_fn = fn_variants.get(dialect.name, fn)
        return dispatched_fn(*args, **kwargs)

    def dialect_decorator(dialect):
        def decorate(dialect_fn):
            if dialect in fn_variants:
                msg = (
                    f"Already defined a variant of function '{fn.__name__}'"
                    f" for dialect '{dialect}'."
                )
                warnings.warn(msg)
            fn_variants[dialect] = dialect_fn
            return dialect_fn

        return decorate

    wrapped.dialect = dialect_decorator
    wrapped.original = fn
    return wrapped
