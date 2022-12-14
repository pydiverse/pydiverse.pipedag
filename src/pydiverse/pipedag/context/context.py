from __future__ import annotations

from contextvars import ContextVar, Token
from threading import Lock
from typing import TYPE_CHECKING, ClassVar

import structlog
from attrs import frozen

if TYPE_CHECKING:
    from pydiverse.pipedag._typing import T
    from pydiverse.pipedag.backend import BaseLockManager
    from pydiverse.pipedag.core import Flow, Stage, Task
    from pydiverse.pipedag.engine.base import Engine
    from pydiverse.pipedag.materialize.store import PipeDAGStore


logger = structlog.get_logger()


class BaseContext:
    _context_var: ClassVar[ContextVar]
    _token: Token = None
    _enter_counter: int = 0
    _lock: Lock = Lock()

    def __enter__(self):
        with self._lock:
            object.__setattr__(self, "_enter_counter", self._enter_counter + 1)
        if self._token is not None:
            return self

        token = self._context_var.set(self)
        object.__setattr__(self, "_token", token)
        return self

    def __exit__(self, *_):
        with self._lock:
            object.__setattr__(self, "_enter_counter", self._enter_counter - 1)
            if self._enter_counter == 0:
                if not self._token:
                    raise RuntimeError
                self._context_var.reset(self._token)
                object.__setattr__(self, "_token", None)
                self.close()

    def close(self):
        """Function that gets called at __exit__"""

    @classmethod
    def get(cls: type[T]) -> T:
        return cls._context_var.get()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_token"]
        del state["_enter_counter"]
        return state


@frozen(slots=False)
class BaseAttrsContext(BaseContext):
    pass


@frozen
class DAGContext(BaseAttrsContext):
    """Context during DAG definition"""

    flow: Flow
    stage: Stage

    _context_var = ContextVar("dag_context")


@frozen
class TaskContext(BaseAttrsContext):
    """Context used while executing a task"""

    task: Task

    _context_var = ContextVar("task_context")


@frozen(slots=False)
class ConfigContext(BaseAttrsContext):
    """Configuration context"""

    config_dict: dict

    name: str
    network_interface: str
    auto_table: tuple[type, ...]
    auto_blob: tuple[type, ...]
    fail_fast: bool

    store: PipeDAGStore
    lock_manager: BaseLockManager
    engine: Engine

    def get_engine(self) -> Engine:
        return self.engine

    def close(self):
        # close all open resources (e.g. kill all database connections)
        if store := self.store:
            store.close()

    def __getstate__(self):
        state = super().__getstate__()
        state.pop("store", None)
        return state

    _context_var = ContextVar("config_context")
