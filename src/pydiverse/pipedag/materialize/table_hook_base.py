# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import sys
from abc import ABC, abstractmethod
from collections.abc import Iterable
from enum import Enum
from typing import Any, Generic

from typing_extensions import Self

from pydiverse.common.util import requires
from pydiverse.common.util.computation_tracing import ComputationTracer
from pydiverse.pipedag import Table
from pydiverse.pipedag._typing import T, TableHookResolverT
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.errors import StoreIncompatibleException
from pydiverse.pipedag.materialize.materializing_task import (
    AutoVersionSupport,
    MaterializingTask,
)


class CanMatResult(Enum):
    NO = 0
    YES = 1
    # Don't assume that other tables of same obj type can be materialized the same way
    YES_BUT_DONT_CACHE = 2

    @staticmethod
    def new(bool_val: bool):
        return CanMatResult.YES if bool_val else CanMatResult.NO


class CanRetResult(Enum):
    NO = 0
    YES = 1
    # For store and type combination it is expected that no hook can retrieve
    NO_HOOK_IS_EXPECTED = 2

    @staticmethod
    def new(bool_val: bool):
        return CanRetResult.YES if bool_val else CanRetResult.NO


class TableHook(Generic[TableHookResolverT], ABC):
    """Base class to define how to handle a specific table

    For more information, take a look at the `BaseTableStore.register_table`
    documentation.
    """

    __slots__ = ()
    auto_version_support: AutoVersionSupport = AutoVersionSupport.NONE

    @classmethod
    @abstractmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        """
        Return `True` if this hook can materialize a table with the specified
        underlying type. If `True` is returned, the `materialize` method
        MUST be implemented for the type.
        """

    @classmethod
    @abstractmethod
    def can_retrieve(cls, type_: type) -> CanRetResult:
        """
        Return `CanRetResult.YES` if this hook can retrieve a table from the store
        and convert it to the specified type. If `CanRetResult.YES` is returned, the
        `retrieve` method MUST be implemented for the type.

        If `CanRetResult.NO_HOOK_IS_EXPECTED` is returned, it is expected that no
        hook can retrieve the table as the type. This is used in parquet table cache
        which does not make sense for SQL references to suppress a warning.
        """

    @classmethod
    def retrieve_as_reference(cls, type_: type) -> bool:
        """
        Table hooks that retrieve `type_` as reference instead of copying the content
        of the table should return True. This is used by imperative materialization
        to determine whether input_type is a good default for returned references.
        """
        return False

    @classmethod
    @abstractmethod
    def materialize(
        cls,
        store: TableHookResolverT,
        table: Table,
        stage_name: str,
        without_config_context: bool = False,
    ) -> None:
        """Materialize a table object

        :param store: The store which called this method
        :param table: The table that should be materialized
        :param stage_name: The name of the stage in which the table should
            be stored - can either be `stage.name` or `stage.transaction_name`.
        """

    @classmethod
    @abstractmethod
    def retrieve(
        cls,
        store: TableHookResolverT,
        table: Table,
        stage_name: str | None,
        as_type: type[T] | tuple | dict[str, Any],
        limit: int | None = None,
    ) -> T:
        """Retrieve a table from the store

        :param store: The store in which the table is stored
        :param table: The table which should get retrieved
        :param stage_name: The name of the stage from which te table should
            be retrieved
        :param as_type: The type as which the table is to be retrieved
        :param limit: The maximum number of rows to retrieve
            (won't work for SQL references types)
        :return: The retrieved table (converted to the correct type)
        """

    @classmethod
    def auto_table(cls, obj: T) -> Table[T]:
        """Wrap an object inside a `Table`

        Given an object that can be materialized by this hook, produces a
        Table instance that may contain additional metadata. This is useful
        to enable automatic name propagation.

        :param obj: The object which should get wrapped inside a `Table`
        :return: The `Table` object which wraps `obj`
        """
        return Table(obj)

    @classmethod
    def lazy_query_str(cls, store: TableHookResolverT, obj) -> str:
        """String that represents the associated object

        Can either be a literal query string (e.g. SQL query), or a string that
        uniquely represents the query in some other way. Used for computing
        the cache key for materializing tasks with `lazy=True`.

        :raises TypeError: if the type doesn't support lazy queries.
        """
        raise TypeError(f"Lazy query not supported with object of type {type(obj)}")

    @classmethod
    def retrieve_for_auto_versioning_lazy(
        cls,
        store: TableHookResolverT,
        table: Table,
        stage_name: str,
        as_type: type[T] | tuple | dict[str, Any],
    ) -> T:
        """
        Retrieve a table (or table like object) from the store to be used for
        automatic version number determination.

        This function gets called with the same arguments as ``.retrieve``.

        Must be implemented for ``auto_version_support == AutoVersionSupport.LAZY``.

        :raises TypeError: if the type doesn't support automatic versioning.
        """
        raise TypeError(f"Auto versioning not supported for type {as_type}")

    @classmethod
    def get_auto_version_lazy(cls, obj) -> str:
        """
        Must be implemented for ``auto_version_support == AutoVersionSupport.LAZY``.

        :param obj: object returned from task
        :return: string representation of the operations performed on this object.
        :raises TypeError: if the object doesn't support automatic versioning.
        """
        raise NotImplementedError

    @classmethod
    def get_computation_tracer(cls) -> ComputationTracer:
        """
        Get a computation tracer for this type of task.
        Must be implemented for ``auto_version_support == AutoVersionSupport.TRACE``.
        """
        raise NotImplementedError


class TableHookResolver:
    class ClassState:
        def __init__(self):
            self.registered_table_hooks: list[type[TableHook]] = []
            self.m_hook_cache: dict[type, type[TableHook]] = {}
            self.r_hook_cache: dict[type, type[TableHook]] = {}
            self.hook_subclass_cache: dict[type, type[TableHook]] = {}

        def add_hook(self, hook_cls, replace_hooks: list[type[TableHook]] | None):
            # we ignore if replace_hooks are not found since they might be found in
            # base class with lower lookup priority
            if replace_hooks:
                self.registered_table_hooks = [x for x in self.registered_table_hooks if x not in replace_hooks]
            self.registered_table_hooks.append(hook_cls)

        def __repr__(self):
            return "ClassState=" + str(
                {x: getattr(self, x) for x in dir(self) if not x.startswith("_") and x != "add_hook"}
            )

    _states: dict[int, ClassState] = {}

    @classmethod
    def _resolver_state(cls: "TableHookResolver"):
        state = cls._states.get(id(cls))
        if state is None:
            state = cls._states[id(cls)] = cls.ClassState()
        return state

    @classmethod
    def register_table(
        cls: "TableHookResolver",
        *requirements: Any,
        replace_hooks: list[type[TableHook]] | None = None,
    ):
        """Decorator to register a `TableHook`

        Each table store should be able to handle tables of various different
        types (e.g. a pandas dataframe, a sqlalchemy select statement, a
        pydiverse transform table, ...). To add table type specific logic to
        a table store so called TableHooks are used.

        This decorator is used to register such a hook::

            try:
                from library import x
            except ImportError as e:
                warnings.warn(str(e), ImportWarning)
                x = None

            @TableStore.register_table(x)
            def XTableHook(TableHook):
                ...

        :param requirements: The requirements which must be satisfied to register
            the decorated class.
        :param replace_hooks: Takes a list of TableHook classes (e.g. PandasTableHook)
            that should be replaced by the decorated class.
            If None, the new hook is just added to the list of available hooks.

            Details: specifying replace_hooks helps also implicitly because the
            mentioned hooks must have been imported/registered before. If they were
            registered on a base class of the hook resolver (i.e. table store) of this
            call, they will be ignored but hooks registered on subclasses take
            precedence anyway. Please make sure not provide replace hooks registered to
            subclasses of this call's hook resolver. This will fail.
        """

        def decorator(hook_cls):
            if not all(requirements):
                cls.__hooks_with_unmet_requirements.append(hook_cls.__module__ + "." + hook_cls.__qualname__)
                return requires(
                    False,
                    Exception(f"Not all requirements met for {hook_cls.__name__}"),
                )(hook_cls)

            cls._resolver_state().add_hook(hook_cls, replace_hooks)
            return hook_cls

        return decorator

    def __all_registered_table_hooks(self) -> Iterable[type[TableHook]]:
        # Walk up the hierarchy of super classes and return each registered
        # table hook in reverse order
        for cls in type(self).__mro__:
            if issubclass(cls, TableHookResolver):
                yield from cls._resolver_state().registered_table_hooks[::-1]  # noqa

    def get_m_table_hook(self: Self, tbl: Table) -> type[TableHook[Self]]:
        """Get a table hook that can materialize the specified type"""
        type_ = type(tbl.obj)
        if type_ in self._resolver_state().m_hook_cache:
            return self._resolver_state().m_hook_cache[type_]

        for hook in self.__all_registered_table_hooks():
            can = hook.can_materialize(tbl)
            if can != CanMatResult.NO and can is not False:
                if can == CanMatResult.YES:
                    self._resolver_state().m_hook_cache[type_] = hook
                return hook

        raise TypeError(
            f"Can't materialize Table with underlying type {type_}. " + self.__hook_unmet_requirements_message()
        )

    def get_r_table_hook(self: Self, type_: type[T] | tuple | dict) -> type[TableHook[Self]] | None:
        """Get a table hook that can retrieve the specified type"""
        if isinstance(type_, tuple):
            type_ = type_[0]
        elif isinstance(type_, dict):
            type_ = type_["type"]

        if type_ is None:
            raise ValueError("type_ argument can't be None")

        if type_ in self._resolver_state().r_hook_cache:
            return self._resolver_state().r_hook_cache[type_]

        for hook in self.__all_registered_table_hooks():
            can_ret = hook.can_retrieve(type_)
            if can_ret == CanRetResult.YES:
                self._resolver_state().r_hook_cache[type_] = hook
                return hook
            elif can_ret == CanRetResult.NO_HOOK_IS_EXPECTED:
                raise StoreIncompatibleException(
                    f"Can't retrieve Table as type {type_}. This type is incompatible with store {self}."
                )

        raise TypeError(f"Can't retrieve Table as type {type_}. " + self.__hook_unmet_requirements_message())

    def get_hook_subclass(self: Self, type_: type[T]) -> type[T]:
        """Finds a table hook that is a subclass of the provided type"""
        if type_ in self._resolver_state().hook_subclass_cache:
            return self._resolver_state().hook_subclass_cache[type_]

        min_distance = sys.maxsize
        min_subclass = None

        for cls in type(self).__mro__:
            if issubclass(cls, TableHookResolver):
                for hook in cls._resolver_state().registered_table_hooks[::-1]:  # noqa
                    if issubclass(hook, type_):
                        # Calculate distance between superclass and child class
                        try:
                            dist = hook.__mro__.index(type_)
                            if dist < min_distance:
                                min_distance = dist
                                min_subclass = hook
                        except ValueError:
                            pass

                if min_subclass:
                    break

        if min_subclass:
            self._resolver_state().hook_subclass_cache[type_] = min_subclass
            return min_subclass

        raise RuntimeError

    def store_table(self, table: Table, task: MaterializingTask | None):
        """Stores a table in the associated transaction stage

        The store must convert the table object (`table.obj`) to the correct
        internal type. This means, that in some cases it first has to
        evaluate a lazy object. For example: if a sql based table store
        receives a sql query to store, it has to execute it first.

        The implementation details of this get handled by the registered
        TableHooks.
        """

        # In case of deferred operations, inform run context that stage
        # isn't 100% cache valid anymore.
        if task is not None:
            RunContext.get().set_stage_has_changed(task.stage)

        # Materialize
        hook = self.get_m_table_hook(table)
        hook.materialize(self, table, table.stage.transaction_name)

    def retrieve_table_obj(
        self,
        table: Table,
        as_type: type[T],
        for_auto_versioning: bool = False,
    ) -> T:
        """Loads a table from the store

        Retrieves the table from the store, converts it to the correct
        If the stage hasn't yet been committed, the table must be retrieved
        from the transaction, else it must be retrieved from the committed
        stage.

        :raises TypeError: if the retrieved table can't be converted to
            the requested type.
        """

        if as_type is None:
            raise TypeError("Missing 'as_type' argument. You must specify a type to be able to dematerialize a Table.")

        hook = self.get_r_table_hook(as_type)
        stage_name = table.stage.current_name if table.stage is not None else None
        try:
            if for_auto_versioning:
                return hook.retrieve_for_auto_versioning_lazy(self, table, stage_name, as_type)

            return hook.retrieve(self, table, stage_name, as_type)
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve table '{table}'") from e

    __hooks_with_unmet_requirements: list[str] = []

    @classmethod
    def __hook_unmet_requirements_message(cls) -> str:
        if len(cls.__hooks_with_unmet_requirements) == 0:
            return "This is is because no TableHook has been registered for this type."

        return (
            "This is either because no TableHook has been registered for this type, "
            "or because not all requirements have been met for the corresponding hook."
            "\nHooks with unmet requirements: " + ", ".join(cls.__hooks_with_unmet_requirements)
        )
