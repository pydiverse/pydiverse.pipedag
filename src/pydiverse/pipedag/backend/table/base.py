from __future__ import annotations

from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Generic, Type

from typing_extensions import Self

from pydiverse.pipedag._typing import StoreT, T
from pydiverse.pipedag.backend.metadata import TaskMetadata
from pydiverse.pipedag.core import MaterialisingTask, Schema, Table
from pydiverse.pipedag.util import requires


class _TableStoreMeta(ABCMeta):
    """TableStore Metaclass

    Add the `_REGISTERED_TABLES`, `_M_TABLE_CACHE` and `_R_TABLE_CACHE`
    attributes to all table stores.
    """

    def __new__(mcs, name, bases, attrs, **kwargs):
        cls = super().__new__(mcs, name, bases, attrs, **kwargs)

        cls._REGISTERED_TABLES = []
        cls._M_TABLE_CACHE = {}
        cls._R_TABLE_CACHE = {}

        return cls


class BaseTableStore(metaclass=_TableStoreMeta):
    """Table store base class

    The table store is responsible for storing and retrieving various types
    of tabular data. Additionally, it also has to manage all task metadata,
    This includes storing it, but also cleaning up stale metadata.

    A store must use a table's name (`table.name`) and schema (`table.schema`)
    as the primary keys for storing and retrieving it. This means that
    two different `Table` objects can be used to store and retrieve the same
    data as long as they have the same name and schema.

    The same is also true for the task metadata where the task `schema`,
    `version` and `cache_key` act as the primary keys (those values are
    stored both in the task object and the metadata object).
    """

    _REGISTERED_TABLES: list[TableHook]
    _M_TABLE_CACHE: dict[type, TableHook]
    _R_TABLE_CACHE: dict[type, TableHook]

    def setup(self):
        """Setup function

        This function gets called by the PipeDAGStore when it gets
        initialised. Unlike the __init__ method, a lock is acquired before
        the setup method gets called to prevent race conditions.
        """

    @classmethod
    def register_table(cls, *requirements: Any):
        """Decorator to register a `TableHook`

        Each table store should be able to handle tables of various different
        types (e.g. a pandas dataframe, a sqlalchemy select statement, a
        pydiverse transform table, ...). To add table type specific logic to
        a table store so called TableHooks are used.

        This decorator is used to register such a hook:
        ::
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
        """

        def decorator(hook_cls):
            if not all(requirements):
                return requires(
                    False,
                    Exception(f"Not all requirements met for {hook_cls.__name__}"),
                )(hook_cls)

            # Register the hook
            cls._REGISTERED_TABLES.append(hook_cls)
            cls._M_TABLE_CACHE.clear()
            cls._R_TABLE_CACHE.clear()
            return hook_cls

        return decorator

    def get_m_table_hook(self: Self, type_: type[T]) -> TableHook[Self]:
        """Get a table hook that can materialise the specified type"""
        if type_ in self._M_TABLE_CACHE:
            return self._M_TABLE_CACHE[type_]
        for hook in self._REGISTERED_TABLES:
            if hook.can_materialise(type_):
                self._M_TABLE_CACHE[type_] = hook
                return hook
        raise TypeError(f"Can't materialise Table with underlying type {type_}")

    def get_r_table_hook(self: Self, type_: type[T]) -> TableHook[Self]:
        """Get a table hook that can retrieve the specified type"""
        if type_ in self._R_TABLE_CACHE:
            return self._R_TABLE_CACHE[type_]
        for hook in self._REGISTERED_TABLES:
            if hook.can_retrieve(type_):
                self._R_TABLE_CACHE[type_] = hook
                return hook
        raise TypeError(f"Can't retrieve Table as type {type_}")

    @abstractmethod
    def create_schema(self, schema: Schema):
        """Creates a schema

        Ensures that the base schema exists (but doesn't clear it) and that
        the working schema exists and is empty.
        """

    @abstractmethod
    def swap_schema(self, schema: Schema):
        """Swap the base schema with the working schema

        After the schema swap the contents of the base schema should be in the
        working schema, and the contents of the working schema in the base
        schema.

        Additionally, the metadata associated with the working schema should
        replace the metadata of the base schema. The latter can be discarded.
        """

    @abstractmethod
    def store_table(self, table: Table, lazy: bool):
        """Stores a table in the associated working schema.

        The store must convert the table object (`table.obj`) to the correct
        internal type. This means, that in some cases it first has to
        evaluate a lazy object. For example: if a sql based table store
        receives a sql query to store, it has to execute it first.

        If `lazy` is set to `True` and the table object represents a lazy
        table / query, the store may choose to check if the same query
        with the same inputs (based on `table.cache_key`) has already been
        executed before. If yes, instead of evaluating the query, it can
        just copy the previous result to the working schema.
        """

    @abstractmethod
    def copy_table_to_working_schema(self, table: Table):
        """Copy a table from the base schema to the working schema

        This operation MUST not remove the table from the base schema or modify
        it in any way.

        :raises CacheError: if the table can't be found in the base schema
        """

    @abstractmethod
    def retrieve_table_obj(
        self, table: Table, as_type: type[T], from_cache: bool = False
    ) -> T:
        """Loads a table from the store

        Retrieves the table from the store, converts it to the correct
        type (given by the `as_type` argument) and returns it.
        If `from_cache` is `False` (default), the table must be retrieved
        from the current schema (`table.schema.current_name`). Before a
        schema swap this corresponds to the working schema and afterwards
        to the base schema. If `from_cache` is `True`, it must always be
        retrieved from the base schema.

        :raises TypeError: if the retrieved table can't be converted to
            the requested type.
        """

    @abstractmethod
    def store_task_metadata(self, metadata: TaskMetadata, schema: Schema):
        """Stores the metadata of a task

        The metadata should always be stored associated in such a way that
        it is associated with the working schema.
        """

    @abstractmethod
    def copy_task_metadata_to_working_schema(self, task: MaterialisingTask):
        """Copy a task's metadata from the base to the working schema

        The schema of a task can be accessed using `task.schema`.
        """

    @abstractmethod
    def retrieve_task_metadata(self, task: MaterialisingTask) -> TaskMetadata:
        """Retrieve a task's metadata from the store

        :raises CacheError: if no metadata for this task can be found.
        """


class TableHook(Generic[StoreT], ABC):
    """Base class to define how to handle a specific table

    For more information, take a look at the `BaseTableStore.register_table`
    documentation.
    """

    __slots__ = ()

    @classmethod
    @abstractmethod
    def can_materialise(cls, type_: type) -> bool:
        """
        Return `True` if this hook can materialise a table with the specified
        underlying type. If `True` is returned, the `materialise` method
        MUST be implemented for the type.
        """

    @classmethod
    @abstractmethod
    def can_retrieve(cls, type_: type) -> bool:
        """
        Return `True` if this hook can retrieve a table from the store
        and convert it to the specified type. If `True` is returned, the
        `retrieve` method MUST be implemented for the type.
        """

    @classmethod
    @abstractmethod
    def materialise(cls, store: StoreT, table: Table, schema_name: str) -> None:
        """Materialise a table object

        :param store: The store which called this method
        :param table: The table that should be materialised
        :param schema_name: The name of the schema in which the table should
            be stored
        """

    @classmethod
    @abstractmethod
    def retrieve(
        cls, store: StoreT, table: Table, schema_name: str, as_type: type[T]
    ) -> T:
        """Retrieve a table from the store

        :param store: The store in which the table is stored
        :param table: The table which should get retrieved
        :param schema_name: The name of the schema from which te table should
            be retrieved
        :param as_type: The type as which the table is to be retrieved
        :return: The retrieved table (converted to the correct type)
        """

    @classmethod
    def auto_table(cls, obj: T) -> Table[T]:
        """Wrap an object inside a `Table`

        Given an object that can be materialised by this hook, produces a
        Table instance that may contain additional metadata. This is useful
        to enable automatic name propagation.

        :param obj: The object which should get wrapped inside a `Table`
        :return: The `Table` object which wraps `obj`
        """
        return Table(obj)
