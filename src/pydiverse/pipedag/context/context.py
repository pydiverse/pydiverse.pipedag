from __future__ import annotations

from contextvars import ContextVar, Token
from enum import Enum
from functools import cached_property
from threading import Lock
from typing import TYPE_CHECKING, Any, ClassVar

from pydiverse.pipedag.util.import_ import import_object, load_object

if TYPE_CHECKING:
    from pydiverse.pipedag._typing import T
    from pydiverse.pipedag.backend import BaseLockManager
    from pydiverse.pipedag.context.run_context import StageLockStateHandler
    from pydiverse.pipedag.core import Flow, Stage, Task
    from pydiverse.pipedag.engine.base import OrchestrationEngine
    from pydiverse.pipedag.materialize.metadata import TaskMetadata

import structlog
from attrs import define, evolve, frozen


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


@define
class TaskContext(BaseContext):
    """Context used while executing a task

    It is used to retrieve a reference to the task object from within a running task.
    It also serves as a place to store temporary state while processing a task.
    """

    task: Task

    # Temporary state associated with a task during materialization
    input_hash: str = None
    cache_fn_hash: str = None
    cache_metadata: TaskMetadata = None

    _context_var = ContextVar("task_context")


class StageCommitTechnique(Enum):
    SCHEMA_SWAP = 0
    READ_VIEWS = 1


@frozen(slots=False)
class ConfigContext(BaseAttrsContext):
    """
    Configuration context for running a particular pipedag instance.

    For the most part it holds serializable attributes. But it also offers access
    to blob_store and table_store as well as lock manager and orchestration engine.
    Lock manager and orchestration engine are managed full cycle by exactly one
    caller (ServerRunContext / Flow.run()). So this class just offers a way to create
    a disposable object. Blob_store and table_store on the other hand might be
    accessed by multi-threaded / multi-processed / multi-node way of orchestrating
    functions in the pipedag. Since they don't keep real state, they can be
    thrown away while pickling and will be reloaded on first access of the
    @cached_property store. Calling ConfigContext.dispose() will close all
    connections and render this object unusable afterwards.
    """

    config_dict: dict

    # names
    pipedag_name: str
    flow_name: str
    instance_name: str

    # per instance attributes
    fail_fast: bool
    strict_result_get_locking: bool
    ignore_task_version: bool
    instance_id: str  # may be used as database name or locking ID
    stage_commit_technique: StageCommitTechnique
    network_interface: str
    attrs: dict[str, Any]

    # other configuration options
    ignore_fresh_input: bool = False

    # INTERNAL FLAGS - ONLY FOR PIPEDAG USE
    # When set to True, exceptions raised in a flow don't get logged
    _swallow_exceptions: bool = False

    @cached_property
    def auto_table(self) -> tuple[type, ...]:
        return tuple(map(import_object, self.config_dict.get("auto_table", ())))

    @cached_property
    def auto_blob(self) -> tuple[type, ...]:
        return tuple(map(import_object, self.config_dict.get("auto_blob", ())))

    @cached_property
    def store(self):
        # Load objects referenced in config
        try:
            from pydiverse.pipedag.backend.table_cache import LocalTableCache

            table_store = load_object(self.config_dict["table_store"])
            if "local_table_cache" in self.config_dict["table_store"]:
                local_cache_cfg = self.config_dict["table_store"]["local_table_cache"]
                local_table_cache = LocalTableCache(
                    load_object(local_cache_cfg),
                    local_cache_cfg.get("store_input", True),
                    local_cache_cfg.get("store_output", False),
                    local_cache_cfg.get("use_stored_input_as_cache", True),
                )
                unknown_attributes = set(local_cache_cfg.keys()) - {
                    "class",
                    "args",
                    "store_input",
                    "store_output",
                    "use_stored_input_as_cache",
                }
                if len(unknown_attributes) > 0:
                    raise AttributeError(
                        (
                            "Unknown attributes in local_table_cache config:"
                            f" {unknown_attributes}"
                        ),
                    )
            else:
                local_table_cache = LocalTableCache(
                    None,
                    store_input=False,
                    store_output=False,
                    use_stored_input_as_cache=False,
                )
        except Exception as e:
            raise RuntimeError("Failed loading table_store") from e

        try:
            blob_store = load_object(self.config_dict["blob_store"])
        except Exception as e:
            raise RuntimeError("Failed loading blob_store") from e
        from pydiverse.pipedag.materialize.store import PipeDAGStore

        return PipeDAGStore(
            table=table_store,
            blob=blob_store,
            local_table_cache=local_table_cache,
        )

    def evolve(self, **changes) -> ConfigContext:
        """Create a new instance with the changes applied; Wrapper for attrs.evolve"""
        return evolve(self, **changes)

    def create_lock_manager(self) -> BaseLockManager:
        return load_object(self.config_dict["lock_manager"])

    def create_orchestration_engine(self) -> OrchestrationEngine:
        return load_object(self.config_dict["orchestration"])

    def close(self):
        # If the store has been initialized (and thus cached in the __dict__),
        # dispose of it, and remove it from the cache.
        if store := self.__dict__.get("store", None):
            store.dispose()
            self.__dict__.pop("store")

    def __getstate__(self):
        state = super().__getstate__()
        # store is not serializable, but @cached_property will reload
        # it from config_dict
        state.pop("store", None)
        state.pop("auto_table", None)
        state.pop("auto_blob", None)
        return state

    _context_var = ContextVar("config_context")


class StageLockContext(BaseContext):
    """
    Context manager used to keep stages locked until after flow.run() has been called
    """

    lock_state_handlers: [StageLockStateHandler]

    _context_var = ContextVar("stage_lock_context")

    def __init__(self):
        self.logger = structlog.get_logger(logger_name=type(self).__name__, id=id(self))
        self.logger.info("Open stage lock context")
        self.lock_state_handlers = []

    def close(self):
        self.logger.info("Close stage lock context")
        for lock_state_handler in self.lock_state_handlers:
            lock_state_handler.dispose()
