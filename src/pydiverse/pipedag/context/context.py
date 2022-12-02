from __future__ import annotations

from contextvars import ContextVar, Token
from threading import Lock
from typing import TYPE_CHECKING, Any, ClassVar

import structlog
from attrs import frozen

if TYPE_CHECKING:
    from pydiverse.pipedag._typing import T
    from pydiverse.pipedag.backend import BaseLockManager
    from pydiverse.pipedag.core import Flow, Stage, Task
    from pydiverse.pipedag.engine.base import OrchestrationEngine
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

            if self._enter_counter == 1:
                self.open()
        return self

    def __exit__(self, *_):
        with self._lock:
            object.__setattr__(self, "_enter_counter", self._enter_counter - 1)
            if self._enter_counter == 0:
                if not self._token:
                    raise RuntimeError
                self.close()
                self._context_var.reset(self._token)
                object.__setattr__(self, "_token", None)

    def open(self):
        """Function that gets called at __enter__"""

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

    pipedag_name: str
    flow_name: str
    instance_name: str
    instance_id: str  # may be used as database name for example
    auto_table: tuple[type, ...]
    auto_blob: tuple[type, ...]
    fail_fast: bool
    # per instance attributes
    network_interface: str
    attrs: dict[str, Any]

    store: PipeDAGStore
    lock_manager: BaseLockManager
    orchestration_engine: OrchestrationEngine

    def get_orchestration_engine(self) -> OrchestrationEngine:
        return self.orchestration_engine

    def open(self):
        """Open all non-serializable resources (i.e. database connections)."""
        for opener in [self.store, self.lock_manager, self.orchestration_engine]:
            if opener is not None:
                opener.open()

    def close(self):
        """Close all open resources (i.e. kill all database connections)."""
        for closer in [self.orchestration_engine, self.store, self.lock_manager]:
            if closer is not None:
                closer.close()

    # # this should not be needed any more since store is opened/closed just like lock_manager and orchestration_engine
    # def __getstate__(self):
    #     state = super().__getstate__()
    #     state.pop("store", None)
    #     return state

    _context_var = ContextVar("config_context")
