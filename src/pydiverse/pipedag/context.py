from __future__ import annotations

import datetime
import threading
import uuid
from collections import defaultdict
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, ClassVar

from attrs import Factory, define

from pydiverse.pipedag._typing import T

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Flow, Stage, Task


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


@define
class RunContext(BaseContext):
    """Context used while executing a flow"""

    flow: Flow
    run_id: str = Factory(lambda: uuid.uuid4().hex[:20])

    created_stages: set[Stage] = Factory(set)
    table_names: defaultdict[Stage, set[str]] = Factory(lambda: defaultdict(set))
    blob_names: defaultdict[Stage, set[str]] = Factory(lambda: defaultdict(set))

    lock: threading.RLock = Factory(threading.RLock)
    _context_var = ContextVar("run_context")


@define
class TaskContext(BaseContext):
    """Context used while executing a task"""

    task: Task

    _context_var = ContextVar("task_context")
