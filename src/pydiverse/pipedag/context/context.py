from __future__ import annotations

from contextvars import ContextVar, Token
from functools import cached_property
from threading import Lock
from typing import TYPE_CHECKING, Any, ClassVar

from pydiverse.pipedag.util.import_ import import_object

if TYPE_CHECKING:
    from pydiverse.pipedag._typing import T
    from pydiverse.pipedag.backend import BaseLockManager
    from pydiverse.pipedag.engine.base import OrchestrationEngine
    from pydiverse.pipedag.core import Flow, Stage, Task

import structlog
from attrs import frozen

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
    """
    Configuration context for running a particular pipedag instance.

    For the most part it holds serializable attributes. But it also offers access to blob_store and table_store as
    well as lock manager and orchestration engine. Lock manager and orchestration engine are managed full cycle by
    exactly one caller (ServerRunContext / Flow.run()). So this class just offers a way to create a disposable object.
    Blob_store and table_store on the other hand might be accessed by multi-threaded / multi-processed / multi-node
    way of orchestrating functions in the pipedag. Since they don't keep real state, they can be thrown away while
    pickling and will be reloaded on first access of the @cached_property store. Calling ConfigContext.dispose() will
    close all connections and render this object unusable afterwards.
    """

    config_dict: dict

    # names
    pipedag_name: str
    flow_name: str
    instance_name: str
    # per instance attributes
    fail_fast: bool
    strict_result_get_locking: bool
    instance_id: str  # may be used as database name or locking ID
    network_interface: str
    attrs: dict[str, Any]

    @cached_property
    def auto_table(self) -> tuple[type, ...]:
        return tuple(map(import_object, self.config_dict.get("auto_table", ())))

    @cached_property
    def auto_blob(self) -> tuple[type, ...]:
        return tuple(map(import_object, self.config_dict.get("auto_blob", ())))

    @cached_property
    def store(self):
        # Load objects referenced in config
        table_store = load_object(self.config_dict["table_store"], self)
        blob_store = load_object(self.config_dict["blob_store"], self)
        from pydiverse.pipedag.materialize.store import PipeDAGStore

        return PipeDAGStore(
            table=table_store,
            blob=blob_store,
        )

    def create_lock_manager(self) -> BaseLockManager:
        return load_object(self.config_dict["lock_manager"], self)

    def create_orchestration_engine(self) -> OrchestrationEngine:
        return load_object(self.config_dict["orchestration"], self)

    def close(self):
        # If the store has been initialized (and thus cached in the __dict__),
        # dispose of it, and remove it from the cache.
        if store := self.__dict__.get("store", None):
            store.dispose()
            self.__dict__.pop("store")

    def __getstate__(self):
        state = super().__getstate__()
        # store is not serializable. But @cached_property will reload it from config_dict
        state.pop("store", None)
        state.pop("auto_table", None)
        state.pop("auto_blob", None)
        return state

    _context_var = ContextVar("config_context")


def load_object(config_dict: dict, cfg: ConfigContext):
    """Instantiates an instance of an object given

    The import path (module.Class) should be specified as the "class" value
    of the dict. The rest of the dict get used as the instance config.

    If the class defines a `_init_conf_` function, it gets called using the
    config values, otherwise they just get passed to the class initializer.

    >>> # module.Class(argument="value")
    >>> load_instance({
    >>>     "class": "module.Class",
    >>>     "argument": "value",
    >>> }, PipedagConfig.load().get())
    """

    config_dict = config_dict.copy()
    cls = import_object(config_dict.pop("class"))

    try:
        init_conf = getattr(cls, "_init_conf_")
        return init_conf(config_dict, cfg)
    except AttributeError:
        return cls(**config_dict)
