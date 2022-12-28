from __future__ import annotations

import functools
import warnings
from typing import TYPE_CHECKING, Callable, Protocol

from pydiverse.pipedag._typing import CallableT

if TYPE_CHECKING:

    class EngineDispatchedFn(Protocol):
        dialect: Callable[[str], Callable[[CallableT], CallableT]]
        original: CallableT


__all__ = [
    "engine_dispatch",
]


def engine_dispatch(fn: CallableT) -> CallableT | EngineDispatchedFn:
    """Dispatch a function based on `self.engine.dialect.name`."""
    fn_variants = {}

    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        self_or_cls = args[0]
        dialect_name = self_or_cls.engine.dialect.name
        dispatched_fn = fn_variants.get(dialect_name, fn)
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
