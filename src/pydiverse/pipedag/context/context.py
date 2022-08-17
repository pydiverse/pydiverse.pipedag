from __future__ import annotations

from contextvars import ContextVar, Token
from functools import cached_property
from threading import Lock
from typing import TYPE_CHECKING, ClassVar

from attrs import frozen

if TYPE_CHECKING:
    from pydiverse.pipedag._typing import T
    from pydiverse.pipedag.backend import BaseLockManager, PipeDAGStore
    from pydiverse.pipedag.core import Flow, Stage, Task


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
    auto_table: tuple[type, ...]
    auto_blob: tuple[type, ...]

    @cached_property
    def store(self) -> PipeDAGStore:
        from pydiverse.pipedag.backend import PipeDAGStore
        from pydiverse.pipedag.util.config import load_instance

        table_store = load_instance(self.config_dict["table_store"])
        blob_store = load_instance(self.config_dict["blob_store"])

        return PipeDAGStore(
            table=table_store,
            blob=blob_store,
        )

    def get_lock_manager(self) -> BaseLockManager:
        from pydiverse.pipedag.util.config import load_instance

        return load_instance(self.config_dict["lock_manager"])

    @classmethod
    def from_file(cls, path: str = None):
        from pydiverse.pipedag.util import config

        if path is None:
            return config.auto_load_config()
        return config.load_config(path)

    def __getstate__(self):
        state = super().__getstate__()
        state.pop("store", None)
        return state

    _context_var = ContextVar("config_context")
