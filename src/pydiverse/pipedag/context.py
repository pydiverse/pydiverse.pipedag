from __future__ import annotations

from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, ClassVar

from attrs import define

from pydiverse.pipedag._typing import T

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Flow, Stage


@define(frozen=True, slots=False)
class BaseContext:
    _context_var: ClassVar[ContextVar]
    _token: ClassVar[Token | None] = None

    def __enter__(self):
        object.__setattr__(self, "_token", self._context_var.set(self))
        return self

    def __exit__(self, *_):
        if not self._token:
            raise RuntimeError
        self._context_var.reset(self._token)
        object.__setattr__(self, "_token", None)

    @classmethod
    def get(cls: type[T]) -> T:
        return cls._context_var.get()


@define
class DAGContext(BaseContext):
    """Context during DAG definition"""

    flow: Flow
    stage: Stage

    _context_var = ContextVar("dag_context")
