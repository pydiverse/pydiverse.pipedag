from __future__ import annotations

import copy
import threading
from collections.abc import Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from threading import Lock
from typing import TYPE_CHECKING, Any, ClassVar

import structlog
from attrs import define, evolve, field, frozen
from box import Box

from pydiverse.pipedag.util import deep_merge
from pydiverse.pipedag.util.import_ import import_object, load_object
from pydiverse.pipedag.util.naming import NameDisambiguator

if TYPE_CHECKING:
    from pydiverse.pipedag import Flow, GroupNode, Stage, Task, VisualizationStyle
    from pydiverse.pipedag._typing import T
    from pydiverse.pipedag.backend import BaseLockManager
    from pydiverse.pipedag.context.run_context import StageLockStateHandler
    from pydiverse.pipedag.engine.base import OrchestrationEngine
    from pydiverse.pipedag.materialize import Table


class BaseContext:
    _context_var: ClassVar[ContextVar]
    _lock: Lock = Lock()
    _thread_state: dict[int, list[Token]] = {}
    _instance_state: dict[int, int] = {}

    def __enter__(self):
        with self._lock:
            _id = id(self) + (threading.get_ident() << 64)
            if _id not in self._thread_state:
                self._thread_state[_id] = []
            _tokens = self._thread_state[_id]
            token = self._context_var.set(self)
            _tokens.append(token)
            if id(self) not in self._instance_state:
                self._instance_state[id(self)] = 0
            # count threads that entered this context object
            self._instance_state[id(self)] += 1
        return self

    def __exit__(self, *_):
        with self._lock:
            _id = id(self) + (threading.get_ident() << 64)
            _tokens = self._thread_state[_id]
            self._context_var.reset(_tokens.pop())
            if len(_tokens) == 0:
                del self._thread_state[_id]
            self._instance_state[id(self)] -= 1
            if self._instance_state[id(self)] == 0:
                # in case of multi-threading, only close objects once
                del self._instance_state[id(self)]
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
        return state


@frozen(slots=False)
class BaseAttrsContext(BaseContext):
    pass


@frozen
class DAGContext(BaseAttrsContext):
    """Context during DAG definition"""

    flow: Flow
    stage: Stage
    group_node: GroupNode

    _context_var = ContextVar("dag_context")


@define
class TaskContext(BaseContext):
    """Context used while executing a task

    It is used to retrieve a reference to the task object from within a running task.
    It also serves as a place to store temporary state while processing a task.
    """

    task: Task
    input_tables: list[Table] = None
    is_cache_valid: bool | None = None
    name_disambiguator: NameDisambiguator = field(factory=NameDisambiguator)
    override_version: str | None = None
    imperative_materialize_callback = None

    _context_var = ContextVar("task_context")


class StageCommitTechnique(Enum):
    """
    - SCHEMA_SWAP: We prepare output in a `<stage>__tmp` schema and then swap
      schemas for `<stage>` and `<stage>__tmp` with three rename operations.
    - READ_VIEWS: We use two schemas, `<stage>__odd` and `<stage>__even`, and
      fill schema `<stage>` just with views to one of those schemas.
    """

    SCHEMA_SWAP = 0
    READ_VIEWS = 1


class CacheValidationMode(Enum):
    """
    - NORMAL: Normal cache invalidation.
    - ASSERT_NO_FRESH_INPUT: Same as IGNORE_FRESH_INPUT and additionally fail if tasks
      having a cache function would still be executed (change in version or lazy query
      ).
    - IGNORE_FRESH_INPUT: Ignore the output of cache functions that help determine
      the availability of fresh input. With `disable_cache_function=False`, it still
      calls cache functions, so cache invalidation works interchangeably between
      IGNORE_FRESH_INPUT and NORMAL.
    - FORCE_FRESH_INPUT: Consider all cache function outputs as different and thus make
      source tasks cache invalid.
    - FORCE_CACHE_INVALID: Disable caching and thus force all tasks as cache invalid.
      This option implies FORCE_FRESH_INPUT.
    """

    NORMAL = 0
    ASSERT_NO_FRESH_INPUT = 1
    IGNORE_FRESH_INPUT = 2
    FORCE_FRESH_INPUT = 3
    FORCE_CACHE_INVALID = 4


@dataclass(frozen=True)
class GroupNodeConfig:
    label: str | None = None
    tasks: list[str] | None = None
    stages: list[str] | None = None
    style_tag: str | None = None


@dataclass(frozen=True)
class VisualizationConfig:
    styles: dict[str, VisualizationStyle] | None = None
    group_nodes: dict[str, GroupNodeConfig] | None = None


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

    # the actual configuration values
    _config_dict: dict

    # names
    pipedag_name: str
    flow_name: str | None
    instance_name: str

    # per instance attributes
    fail_fast: bool
    strict_result_get_locking: bool
    instance_id: str  # may be used as database name or locking ID
    stage_commit_technique: StageCommitTechnique
    cache_validation: Box
    visualization: dict[str, VisualizationConfig]
    network_interface: str
    disable_kroki: bool
    kroki_url: str | None
    attrs: Box

    table_hook_args: Box

    # INTERNAL FLAGS - ONLY FOR PIPEDAG USE
    # When set to True, exceptions raised in a flow don't get logged
    _swallow_exceptions: bool = False
    _is_evolved: bool = False  # if True, _config_dict might be out of date

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
            # Attention: See SQLTableStore.__new__ for how this call may instantiate
            # a subclass of table_store_config["class"] depending on engine URL.
            # For testing, we also allow setting table_store_config["class"] to an
            # actual class. In case you want to create a table store that does not
            # replace the default derived class for a specific dialect, set
            # `_dialect_name = DISABLE_DIALECT_REGISTRATION`.
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
        dicts = {}
        for name, value in changes.items():
            if isinstance(value, Mapping):
                dicts[name] = deep_merge(getattr(self, name), value)
        changes.update(dicts)
        changes.update(dict(is_evolved=True))
        evolved = evolve(self, **changes)

        # Transfer cached properties
        cached_properties = ["auto_table", "auto_blob", "store"]
        for name in cached_properties:
            if name in self.__dict__:
                evolved.__dict__[name] = self.__dict__[name]
                evolved.__dict__[f"__{name}_inherited"] = True

        return evolved

    def create_lock_manager(self) -> BaseLockManager:
        with self:  # ensure that DatabaseLockManager uses correct engine
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

    @staticmethod
    def new(config: dict[str, Any], pipedag_name: str, flow: str, instance: str):
        """
        Create a new ConfigContext instance from dictionary and a few names.

        :param config:
            dictionary with config values
        :param pipedag_name:
            name of pipedag config
        :param flow:
            name of flow
        :param instance:
            name of instance
        :return:
            ConfigContext instance
        """

        # check enums
        # Alternative: could be a feature of __get_merged_config_dict
        # in case default value is set to Enum
        for parent_cfg, enum_name, enum_type in [
            (config, "stage_commit_technique", StageCommitTechnique),
            (config["cache_validation"], "mode", CacheValidationMode),
        ]:
            parent_cfg[enum_name] = parent_cfg[enum_name].strip().upper()
            if not hasattr(enum_type, parent_cfg[enum_name]):
                raise ValueError(
                    f"Found unknown setting {enum_name}: '{parent_cfg[enum_name]}';"
                    f" Expected one of: {', '.join([v.name for v in enum_type])}"
                )
        stage_commit_technique = getattr(
            StageCommitTechnique, config["stage_commit_technique"]
        )
        cache_validation = copy.deepcopy(config["cache_validation"])
        cache_validation["mode"] = getattr(
            CacheValidationMode, config["cache_validation"]["mode"]
        )
        # parsing visualization dictionaries into objects (this could be generalized
        # and possible an open source solution exists)
        from pydiverse.pipedag import VisualizationStyle

        if not isinstance(config.get("visualization", {}), dict):
            raise ValueError(
                "Config section 'visualization' must be a dictionary, "
                f"found {type(config['visualization'])}"
            )
        visualization = {}
        for key, value in config.get("visualization", {}).items():
            ConfigContext.parse_in_object(
                key, value, VisualizationConfig, "visualization", inout=visualization
            )
            for _field, structure, class_type in [
                ("styles", visualization[key].styles, VisualizationStyle),
                ("group_nodes", visualization[key].group_nodes, GroupNodeConfig),
            ]:
                if structure:
                    for key2, value2 in structure.items():
                        ConfigContext.parse_in_object(
                            key2,
                            value2,
                            class_type,
                            f"visualization.{key}.{_field}",
                            inout=structure,
                        )
        if (
            cache_validation["mode"] == CacheValidationMode.NORMAL
            and cache_validation["disable_cache_function"]
        ):
            raise ValueError(
                "cache_validation.disable_cache_function=True is not allowed in "
                "combination with cache_validation.mode=NORMAL"
            )
        # Construct final ConfigContext
        config_context = ConfigContext(
            config_dict=config,
            pipedag_name=pipedag_name,
            flow_name=flow,
            instance_name=instance,
            fail_fast=config["fail_fast"],
            strict_result_get_locking=config["strict_result_get_locking"],
            instance_id=config["instance_id"],
            stage_commit_technique=stage_commit_technique,
            cache_validation=Box(cache_validation, frozen_box=True),
            visualization=visualization,
            network_interface=config["network_interface"],
            disable_kroki=config.get("disable_kroki"),
            kroki_url=config.get("kroki_url"),
            attrs=Box(config["attrs"], frozen_box=True),
            table_hook_args=Box(
                config["table_store"].get("hook_args", {}), frozen_box=True
            ),
        )
        return config_context

    @staticmethod
    def parse_in_object(
        key: str,
        value: dict[str, Any],
        class_type: Any,
        within: str,
        *,
        inout: dict[str, Any],
    ):
        """
        Parse a dictionary into a dataclass instance.

        :param key:
            key of inout dictionary to be modified
        :param value:
            dictionary to be parsed into a dataclass instance and stored in inout[key]
        :param class_type:
            dataclass type for what should be written to inout[key]
        :param within:
            context for error messages
        :param inout:
            dictionary to be modified by this function call
        :return:
            None
        """
        structure = inout
        if not isinstance(value, dict):
            raise ValueError(
                f"Config section '{key}' within '{within}' must be a dictionary, "
                f"found {type(value)}"
            )
        members = set(class_type.__dataclass_fields__.keys())
        if unexpected := set(value.keys()) - members:
            raise ValueError(
                f"Unexpected keys in section '{key}' within '{within}': "
                f"{unexpected}; expected: '{', '.join(members)}'"
            )
        structure[key] = class_type(
            **{m: copy.copy(value[m]) for m in members if m in value}
        )
        for m in value:
            annotation = class_type.__annotations__[m]
            if annotation.startswith("dict[") and not isinstance(value[m], dict):
                raise ValueError(
                    f"Expected dictionary for '{m}' within '{within}.{key}', "
                    f"found {type(value[m])}"
                )
            if annotation.startswith("bool") and not isinstance(value[m], bool):
                raise ValueError(
                    f"Expected boolean for '{m}' within '{within}.{key}', "
                    f"found {type(value[m])}"
                )
            if annotation.startswith("str") and not isinstance(value[m], str):
                raise ValueError(
                    f"Expected string for '{m}' within '{within}.{key}', "
                    f"found {type(value[m])}"
                )
            if annotation.startswith("int") and not isinstance(value[m], int):
                raise ValueError(
                    f"Expected integer for '{m}' within '{within}.{key}', "
                    f"found {type(value[m])}"
                )

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
