from __future__ import annotations

from contextvars import ContextVar, Token
from enum import Enum
from functools import cached_property
from threading import Lock
from typing import TYPE_CHECKING, ClassVar

import structlog
from attrs import define, evolve, field, frozen
from box import Box

from pydiverse.pipedag.util.import_ import import_object, load_object
from pydiverse.pipedag.util.naming import NameDisambiguator

if TYPE_CHECKING:
    from pydiverse.pipedag._typing import T
    from pydiverse.pipedag.backend import BaseLockManager
    from pydiverse.pipedag.context.run_context import StageLockStateHandler
    from pydiverse.pipedag.core import Flow, Stage, Task
    from pydiverse.pipedag.engine.base import OrchestrationEngine


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
                self._close()

    def _close(self):
        """Function that gets called at __exit__"""

    @classmethod
    def get(cls: type[T]) -> T:
        """Returns the current, innermost context instance.

        :raises LookupError: If no such context has been entered yet.
        """
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
    is_cache_valid: bool | None = None
    name_disambiguator: NameDisambiguator = field(factory=NameDisambiguator)

    _context_var = ContextVar("task_context")


class StageCommitTechnique(Enum):
    SCHEMA_SWAP = 0
    READ_VIEWS = 1


@frozen(slots=False)
class ConfigContext(BaseAttrsContext):
    """Configuration context for running a particular pipedag instance.

    To create a `ConfigContext` instance use :py:meth:`PipedagConfig.get`.

    ..
        For the most part, the Config context holds serializable config attributes.
        But it also offers access to the table and blob store, as well as the lock
        manager and orchestration engine.

        Because we can't pickle those classes (and pipedag supports flow execution
        across multiple processes / nodes), the lock manager and orchestration engine
        only get created once per flow execution and then get managed by a central
        process (in the case of the lock manager this is the RunContextServer).

        The table and blob store on the other hand need to be accessed by many task
        across multiple threads / processes / nodes. Because they don't contain
        any state (all state gets stored in the RunContextServer), they get
        thrown away during pickling and then get recreated on the first access.

    Attributes
    ----------
    pipedag_name :
        :ref:`Name <config-name>` of the config file.
    flow_name :
        Name of the flow used for instantiating this config.
    instance_name :
        Name of the instance used for instantiating this config.
    instance_id :
        The :ref:`instance_id`.
    attrs :
        Values from the :ref:`config-attrs` config section, stored in a |Box|_ object.
        Useful for passing any custom, use case specific config options to your flow.

        .. |Box| replace:: ``Box``
        .. _Box: https://github.com/cdgriffith/Box/wiki
    """

    _config_dict: dict

    # names
    pipedag_name: str
    flow_name: str | None
    instance_name: str

    # per instance attributes
    fail_fast: bool
    strict_result_get_locking: bool
    ignore_task_version: bool
    instance_id: str  # may be used as database name or locking ID
    stage_commit_technique: StageCommitTechnique
    network_interface: str
    attrs: Box

    table_hook_args: Box

    # run specific options
    ignore_fresh_input: bool = False

    # INTERNAL FLAGS - ONLY FOR PIPEDAG USE
    # When set to True, exceptions raised in a flow don't get logged
    _swallow_exceptions: bool = False

    @cached_property
    def auto_table(self) -> tuple[type, ...]:
        return tuple(map(import_object, self._config_dict.get("auto_table", ())))

    @cached_property
    def auto_blob(self) -> tuple[type, ...]:
        return tuple(map(import_object, self._config_dict.get("auto_blob", ())))

    @cached_property
    def store(self):
        from pydiverse.pipedag.materialize.store import PipeDAGStore

        # Load objects referenced in config
        try:
            table_store_config = self._config_dict["table_store"]
            table_store = load_object(table_store_config)
        except Exception as e:
            raise RuntimeError("Failed loading table_store") from e

        try:
            blob_store = load_object(self._config_dict["blob_store"])
        except Exception as e:
            raise RuntimeError("Failed loading blob_store") from e

        try:
            local_table_cache = None
            local_table_cache_config = table_store_config.get("local_table_cache", None)
            if local_table_cache_config is not None:
                local_table_cache = load_object(
                    local_table_cache_config,
                    move_keys_into_args=(
                        "store_input",
                        "store_output",
                        "use_stored_input_as_cache",
                    ),
                )
        except Exception as e:
            raise RuntimeError("Failed loading local_table_cache") from e

        return PipeDAGStore(
            table=table_store,
            blob=blob_store,
            local_table_cache=local_table_cache,
        )

    def evolve(self, **changes) -> ConfigContext:
        """Create a new config context instance with the changes applied.

        Because ConfigContext is immutable, this is the only valid way to derive a
        new instance with some values mutated.

        Wrapper around |attrs.evolve()|_.

        .. |attrs.evolve()| replace:: ``attrs.evolve()``
        .. _attrs.evolve(): https://www.attrs.org/en/stable/api.html#attrs.evolve
        """
        evolved = evolve(self, **changes)

        # Transfer cached properties
        cached_properties = ["auto_table", "auto_blob", "store"]
        for name in cached_properties:
            if name in self.__dict__:
                evolved.__dict__[name] = self.__dict__[name]
                evolved.__dict__[f"__{name}_inherited"] = True

        return evolved

    def create_lock_manager(self) -> BaseLockManager:
        return load_object(self._config_dict["lock_manager"])

    def create_orchestration_engine(self) -> OrchestrationEngine:
        return load_object(self._config_dict["orchestration"])

    def _close(self):
        # If the store has been initialized (and thus cached in the __dict__),
        # dispose of it, and remove it from the cache.
        if store := self.__dict__.get("store", None):
            if not self.__dict__.get("__store_inherited", False):
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
    Context manager used to keep stages locked until after :py:meth:`Flow.run()`.

    By default, pipedag releases stage locks as soon as it is done processing a stage.
    This means that by the time the flow has finished running, another flow
    run might already have overwritten any data in those stages.
    Consequently, calling :py:meth:`Result.get()` might not return the values
    produced by the current run, but instead those from the other (newer) run.

    To prevent this, you can wrap the calls to :py:meth:`Flow.run()` and
    :py:meth:`Result.get()` in a `StageLockContext`. This keeps all stages modified
    by the flow to remain locked until the end of `StageLockContext`.

    Example
    -------
    ::

        with StageLockContext():
            result = flow.run()
            df = result.get(task_x)
    """

    lock_state_handlers: [StageLockStateHandler]

    _context_var = ContextVar("stage_lock_context")

    def __init__(self):
        self.logger = structlog.get_logger(logger_name=type(self).__name__, id=id(self))
        self.logger.info("Open stage lock context")
        self.lock_state_handlers = []

    def _close(self):
        self.logger.info("Close stage lock context")
        for lock_state_handler in self.lock_state_handlers:
            lock_state_handler.dispose()
