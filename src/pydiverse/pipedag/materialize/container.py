from __future__ import annotations

import copy
from collections.abc import Iterable
from functools import total_ordering
from typing import TYPE_CHECKING, Any, Generic

import sqlalchemy as sa
import structlog
from attr import frozen

from pydiverse.pipedag._typing import T
from pydiverse.pipedag.context import ConfigContext, TaskContext
from pydiverse.pipedag.errors import DuplicateNameError
from pydiverse.pipedag.util import normalize_name

if TYPE_CHECKING:
    from pydiverse.pipedag.core.stage import Stage
    from pydiverse.pipedag.materialize.core import MaterializingTask


@total_ordering
class Table(Generic[T]):
    """Container for storing Tables.

    Used to wrap table objects that get returned from materializing
    tasks. Tables get stored using the table store.

    .. code-block:: python
       :caption: Example: How to return a table from a task.

        @materialize()
        def task():
            df = pd.DataFrame({"x": [0, 1, 2, 3]}
            return Table(df, "name")

    :param obj: The table object to wrap
    :param name: Optional, case-insensitive name. If no name is provided,
        an automatically generated name will be used. To prevent name collisions,
        you can append ``"%%"`` at the end of the name to enable automatic
        name mangling.
    :param primary_key: Optional name of the primary key that should be
        used when materializing this table. Only supported by some table stores.
    :param indexes: Optional list of indexes to create. Each provided index should be
        a list of column names. Only supported by some table stores.
    :param type_map: Optional map of column names to types. Depending on the table
        store this will allow you to control the datatype as which the specified
        columns get materialized.
    :param nullable: List of columns that should be nullable. If nullable is not None,
        all other columns will be non-nullable.
    :param non_nullable: List of columns that should be non-nullable. If non_nullable
        is not None, all other columns will be nullable.
    :param materialization_details: The label of the materialization_details to be used.
        Overwrites the label given by the stage.

    .. seealso:: You can specify which types of objects should automatically get
        converted to tables using the :ref:`auto_table` config option.
    """

    def __init__(
        self,
        obj: T | None = None,
        name: str | None = None,
        *,
        primary_key: str | list[str] | None = None,
        indexes: list[list[str]] | None = None,
        type_map: dict[str, Any] | None = None,
        nullable: list[str] | None = None,
        non_nullable: list[str] | None = None,
        materialization_details: str | None = None,
    ):
        self._name = None
        self.stage: Stage | None = None
        self.external_schema: str | None = None
        self.shared_lock_allowed: bool = True

        self.obj = obj
        self.name = name
        self.primary_key = primary_key
        self.indexes = indexes
        self.type_map = type_map
        self.nullable = nullable
        self.non_nullable = non_nullable
        self.materialization_details = materialization_details

        # Check that indexes is of type list[list[str]]
        indexes_type_error = TypeError(
            "Table argument 'indexes' must be of type list[list[str]]. "
            "Make sure you provide a 2d list, not just a 1d list."
        )
        if self.indexes is not None:
            if not isinstance(self.indexes, (list, tuple)):
                raise indexes_type_error
            for index in self.indexes:
                if not isinstance(index, (list, tuple)):
                    raise indexes_type_error
                for col in index:
                    if not isinstance(col, str):
                        raise indexes_type_error
        for arg, name in [
            (self.nullable, "nullable"),
            (self.non_nullable, "non_nullable"),
        ]:
            type_error = TypeError(
                f"Table argument '{name}' must be of type list[str]."
            )
            if arg is not None:
                if not isinstance(arg, Iterable) or isinstance(arg, str):
                    raise type_error
                if not all(isinstance(x, str) for x in arg):
                    raise type_error

        # ExternalTableReference can reference a table from an external schema
        if isinstance(self.obj, ExternalTableReference):
            self.external_schema = self.obj.schema
            if self.name is not None:
                raise ValueError(
                    "When using an ExternalTableReference, the name of the Table must "
                    "be set via the ExternalTableReference."
                )
            self.name = self.obj.name
            self.shared_lock_allowed = self.obj.shared_lock_allowed

        # cache_key will be overridden shortly before handing over to downstream tasks
        # that use it to compute their input_hash for cache_invalidation due to input
        # change
        self.cache_key = None
        # assumed dependencies are filled by imperative materialization to ensure
        # correct cache invalidation
        self.assumed_dependencies: list[Table] | None = None

    def __repr__(self):
        stage_name = self.stage.name if self.stage else None
        return f"<Table '{self.name}' ({stage_name})>"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if value is not None and not isinstance(value, str):
            raise TypeError(f"Table name must be of instance 'str' not {type(value)}.")
        self._name = normalize_name(value)

    def copy_without_obj(self) -> Table:
        obj = self.obj
        self.obj = None
        self_copy = copy.deepcopy(self)
        self.obj = obj
        return self_copy

    def materialize(
        self,
        config_context: ConfigContext | None = None,
        schema: Schema | None = None,
        return_as_type=None,
        return_nothing=False,
    ):
        """Materialize the table.

        This method is used to materialize the table in the table store.
        This imperative way of triggering materialization is useful for debugging
        and it allows materializing subqueries within a task. Every call of this
        function is registered in TaskContext and if any object returned
        by this is returned from an ``@materialize`` task, it will be replaced by
        the respective Table object that was materialized.

        :param config_context: The config context to use for materialization during
            interactive debugging. If it is provided while task is regularly executed,
            it must be identical to ConfigContext.get().
        :param return_as_type: By default, the materialized table is returned as the
            input_type of the task (for dataframes with AUTO_VERSION support, it
            just dematerializes the empty dataframes). With return_as you can
            override this behavior and explicitly choose the dematiarlization type.
            ``return_as=sqlalchemy.Table`` might be a useful choice for tasks with
            pandas or polars input_type. It is also possible to pass an `Iterable` like
            list in order to receive a tuple of the dematerialized objects.
        :param return_nothing: If True, the method will return None instead of the
            dematerialized created table.
        """
        try:
            task_context = (
                TaskContext.get()
            )  # raises Lookup Error if no TaskContext is open
            if config_context is not None and config_context is not ConfigContext.get():
                raise ValueError(
                    "config_context must be identical to ConfigContext.get() "
                    "when task is regularly executed by pipedag orchestration."
                )
            config_context = ConfigContext.get()
            task_schema = config_context.store.table_store.get_schema(
                task_context.task.stage.transaction_name
            )
            if schema is not None and schema != task_schema:
                raise ValueError(
                    "schema must be identical to Task Stage transaction schema "
                    "when task is regularly executed by pipedag orchestration."
                )
            schema = task_schema
            if task_context.imperative_materialize_callback is None:
                # this path is triggered while determining auto-version
                return self.obj
            try:
                return task_context.imperative_materialize_callback(
                    self, config_context, return_as_type, return_nothing
                )
            except (RuntimeError, DuplicateNameError):
                # fall back to debug materialization when Table.materialize() is
                # called twice for the same table
                task_context.task.logger.info(
                    "Falling back to debug materialization due to duplicate "
                    "materializtion of this table"
                )

                def return_type_mutator(return_as_type):
                    if return_as_type is None:
                        task: MaterializingTask = task_context.task  # type: ignore
                        return_as_type = task.input_type
                        if (
                            return_as_type is None
                            or not config_context.store.table_store.get_r_table_hook(
                                return_as_type
                            ).retrieve_as_reference(return_as_type)
                        ):
                            # dematerialize as sa.Table if it would transfer all rows
                            # to python when dematerializing with input_type
                            return_as_type = sa.Table

                if isinstance(return_as_type, Iterable):
                    return_as_type = tuple(
                        return_type_mutator(t) for t in return_as_type
                    )
                else:
                    return_as_type = return_type_mutator(return_as_type)
        except LookupError:
            # LookupError happens if no TaskContext is open
            pass
        if config_context is not None:
            if schema is None:
                raise ValueError(
                    "schema must be provided when task is not regularly "
                    "executed by pipedag orchestration."
                )
            # fall back to debug behavior when an explicit table_store is given
            # via config_context
            from pydiverse.pipedag.materialize import debug

            table_name = debug.materialize_table(
                table=self,
                config_context=config_context,
                schema=schema,
            )
            if not return_nothing:

                def get_return_obj(return_as_type):
                    if return_as_type is None:
                        # use sqlalchemy as default reference dematerialization
                        return_as_type = sa.Table
                    store = config_context.store.table_store
                    hook = store.get_r_table_hook(return_as_type)
                    save_name = self.name
                    self.name = table_name
                    schema_name = (
                        schema.name
                        if store.get_schema(schema.name).get() == schema.get()
                        else schema.get()
                    )
                    if store.get_schema(schema_name).get() != schema.get():
                        raise ValueError(
                            "Schema prefix and postfix must match prefix and postfix of"
                            " provided config_context: "
                            f"{store.get_schema(schema_name).get()} != {schema.get()}"
                        )
                    obj = hook.retrieve(
                        config_context.store.table_store,
                        self,
                        schema_name,
                        return_as_type,
                    )
                    self.name = save_name
                    return obj

                if isinstance(return_as_type, Iterable):
                    return tuple(get_return_obj(t) for t in return_as_type)
                else:
                    return get_return_obj(return_as_type)

        elif not return_nothing:
            # We support calling dataframe tasks outside flow declaration by
            # assuming that input tables are already given dematerialized by the
            # caller and returning dataframe objects unmaterialized. In this scenario,
            # we still like `return Table(...)` to behave identical to
            # `return Table(...).materialize()` at the end of tasks.
            # Furthermore, we can support
            # `df = Table(...).materialize(return_as_type=pd.DataFrame)`
            if return_as_type is not None:
                logger = structlog.get_logger(self.__class__.__name__, table=self)
                logger.info(
                    "Ignoring return_as_type in Table.materialize() outside of flow "
                    "without given config_context.",
                    return_as_type=return_as_type,
                )
            return self.obj

    def __getstate__(self):
        # The table `obj` field can't necessarily be pickled. That's why we remove it
        # from the state before pickling.
        state = self.__dict__.copy()
        state["obj"] = None
        return state

    def __lt__(self, other):
        if self.stage is not None or other.stage is not None:
            if self.stage < other.stage:
                return True
            if self.stage > other.stage:
                return False
        return self.name < other.name

    def __eq__(self, other: Table):
        if not isinstance(other, Table):
            return False
        return self.name == other.name and (
            self.stage == other.stage or (self.stage is None and other.stage is None)
        )

    def __hash__(self):
        return hash(self.name) if self.stage is None else hash((self.name, self.stage))


class RawSql:
    """Container for raw sql strings.

    This allows returning sql query strings that then get executed in the
    table store. This is only intended to help with transitioning legacy sql
    pipelines to pipedag, and should be replaced with pipedag managed tables as
    soon as possible.

    .. attention::
        When using RawSql, make sure that you only write tables to the stage that
        the corresponding task is running in. Otherwise, schema swapping won't work.
        To do this, pass the current stage as an argument to your task and then
        access the current stage name using :py:class:`Stage.current_name`.

    :param sql: The sql query string to execute. Depending on the database dialect
        and the separator parameter, the query will be split into multiple subqueries
        that then get executed sequentially.
    :param name: Optional, case-insensitive name. If no name is provided,
        an automatically generated name will be used. To prevent name collisions,
        you can append ``"%%"`` at the end of the name to enable automatic
        name mangling.
    :param separator: The separator used when splitting the query into subqueries.
        Default: ``";"``

    Example
    -------
    When you want to use tables produced by a RawSql task in a downstream task,
    you can either use square brackets during flow definition to pass
    a specific table to a child task, or you can pass the entire RawSql object
    as an input, in which case all tables get loaded and the child task can
    access them using square brackets (or any of the dict-like methods
    defined by RawSql).

    .. code-block::
        :emphasize-lines: 23-24, 26-27

        @materialize(lazy=True)
        def raw_sql_task(stage):
            schema = stage.current_name
            return RawSql(f\"\"\"
                CREATE TABLE {schema}.tbl_1 AS SELECT 1 as x;
                CREATE TABEL {schema}.tbl_2 AS SELECT 2 as x;
            \"\"\")

        @materialize(input_type=sa.Table)
        def foo(tbl_1, tbl_2):
            ...

        @materialize(input_type=sa.Table)
        def bar(raw_sql):
            tbl_1 = raw_sql["tbl_1"]
            tbl_2 = raw_sql["tbl_2"]
            ...

        with Flow() as f:
            with Stage("stage") as s:
                out = raw_sql_task(s)

                # Manually pass specific tables into the task
                foo(out["tbl_1"], out["tbl_2"])

                # Or pass the entire RawSql object into the task
                bar(out)
    """

    def __init__(
        self,
        sql: str | None = None,
        name: str | None = None,
        separator: str = ";",
    ):
        self._name = None
        self.stage: Stage | None = None

        self.sql = sql
        self.name = name
        self.separator = separator

        # cache_key will be overridden shortly before handing over to downstream tasks
        # that use it to compute their input_hash for cache_invalidation due to input
        # change
        self.cache_key = None

        # assumed dependencies are filled by imperative materialization to ensure
        # correct cache invalidation
        self.assumed_dependencies: list[Table] | None = None

        # If a task receives a RawSQL object as input, it loads all tables
        # produced by it and makes them available through a dict like interface.
        self.table_names: list[str] = None  # type: ignore
        self.loaded_tables: dict[str, Any] = None  # type: ignore

    def __repr__(self):
        stage_name = self.stage.name if self.stage else None
        sql_short = None
        if self.sql:
            sql_short = self.sql.strip()[0:40].replace("\n", "").strip()
        return f"<Raw SQL '{self.name}' ({stage_name}) - {sql_short}>"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if value is not None and not isinstance(value, str):
            raise TypeError(f"RawSql name must be of instance 'str' not {type(value)}.")
        self._name = normalize_name(value)

    # Dict-like interface

    def __iter__(self) -> Iterable[str]:
        """Yields all names of tables produced by this RawSql object."""
        yield from self.table_names

    def __contains__(self, table_name: str) -> bool:
        """Check if this RawSql object produced a table with name `table_name`."""
        return table_name in self.table_names

    def __getitem__(self, table_name: str):
        """Gets the table produced by this RawSql object with name `table_name`."""
        if table_name not in self.table_names:
            raise KeyError(f"No table with name '{table_name}' found in RawSql.")

        # Did load tables -> __getitem__ should return Table.obj
        if self.loaded_tables is not None:
            return self.loaded_tables[table_name]

        # Otherwise, while preparing the task inputs inside
        # (`TaskGetItem.resolve_value`) we need to return objects of type `Table`.
        table = Table(None, table_name)
        table.stage = self.stage
        table.cache_key = self.cache_key
        return table

    def items(self) -> Iterable[tuple[str, Any]]:
        """Returns pairs of ``(table_name, table)``."""
        for table_name in self.table_names:
            yield table_name, self[table_name]

    def get(self, table_name: str, default=None):
        """
        Returns the table with name `table_name`, or if no such table exists,
        the `default` value.
        """
        if table_name in self:
            return table_name
        return default

    def copy_without_obj(self) -> RawSql:
        obj = self.loaded_tables
        self.loaded_tables = None
        self_copy = copy.deepcopy(self)
        self.loaded_tables = obj
        return self_copy

    def __getstate__(self):
        # The `loaded_tables` field can't necessarily be pickled. That's why we
        # remove it from the state before pickling.
        state = self.__dict__.copy()
        state["loaded_tables"] = None
        return state


class Blob(Generic[T]):
    """Blob (binary large object) container.

    Used to wrap arbitrary Python objects that get returned from materializing
    tasks. Blobs get stored in the blob store.

    .. code-block:: python
       :caption: Example: How to return a blob from a task.

        @materialize()
        def task():
            obj = SomePicklableClass()
            return Blob(obj, "name")

    :param obj: The object to wrap
    :param name: Optional, case-insensitive name. If no name is provided,
        an automatically generated name will be used. To prevent name collisions,
        you can append ``"%%"`` at the end of the name to enable automatic
        name mangling.

    .. seealso:: You can specify which types of objects should automatically get
        converted to blobs using the :ref:`auto_blob` config option.
    """

    def __init__(
        self,
        obj: T | None = None,
        name: str | None = None,
    ):
        self._name = None
        self.stage: Stage | None = None

        self.obj = obj
        self.name = name

        # cache_key will be overridden shortly before handing over to downstream tasks
        # that use it to compute their input_hash for cache_invalidation due to input
        # change
        self.cache_key = None

    def __repr__(self):
        stage_name = self.stage.name if self.stage else None
        return f"<Blob '{self.name}' ({stage_name})>"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = normalize_name(value)

    def copy_without_obj(self) -> Blob:
        obj = self.obj
        self.obj = None
        self_copy = copy.deepcopy(self)
        self.obj = obj
        return self_copy


class ExternalTableReference:
    """Reference to a user-created table.

    By returning a `ExternalTableReference` wrapped in a :py:class:`~.Table` from,
    a task you can tell pipedag about a table, a view or DB2 nickname in an external
    `schema`. The schema may be a multi-part identifier like "[db_name].[schema_name]"
    if the database supports this. It is passed to SQLAlchemy as-is.

    Only supported by :py:class:`~.SQLTableStore`.

    Warning
    -------
    When using a `ExternalTableReference`, pipedag has no way of knowing the cache
    validity of the external object. Hence, the user should provide a cache function
    for the `Task`.
    It is now allowed to specify a `ExternalTableReference` to a table in schema of the
    current stage.

    Example
    -------
    You can use a `ExternalTableReference` to tell pipedag about a table that exists
    in an external schema::

        @materialize(version="1.0")
        def task():
            return Table(ExternalTableReference("name_of_table", "schema"))

    By using a cache function, you can establish the cache (in-)validity of the
    external table::

        from datetime import date

        # The external table becomes cache invalid every day at midnight
        def my_cache_fun():
            return date.today().strftime("%Y/%m/%d")

        @materialize(cache=my_cache_fun)
        def task():
            return Table(ExternalTableReference("name_of_table", "schema"))

    The ExternalTableReference object can also be created at flow wiring time::

        with Flow() as f:
            with Stage("stage") as s:
                tbl = Table(ExternalTableReference("name_of_table", "schema"))
                _ = some_task(tbl)
    """

    def __init__(self, name: str, schema: str, shared_lock_allowed: bool = False):
        """
        :param name: The name of the table, view, or nickname/alias
        :param schema: The external schema of the object. A multi-part schema
            is allowed with '.' separator as also supported by SQLAlchemy Table
            schema argument.
        :param shared_lock_allowed: Whether to disable acquiring a shared lock
            when using the object in a SQL query. If set to `False`, no lock is
            used. This is useful when the user is not allowed to lock the table.
            If pipedag does not lock source tables for this dialect, this argument
            has no effect. The default is `False`.
        """
        self.name = name
        self.schema = schema
        self.shared_lock_allowed = shared_lock_allowed

    def __repr__(self):
        return f"<ExternalTableReference: {hex(id(self))}" f" (schema: {self.schema})>"


@frozen
class Schema:
    """
    Class for holding a schema name with separable prefix and suffix.

    Attributes
    ----------
    name : str
        The schema name.
    prefix : str
        The prefix to be added to the schema name.
    suffix : str
        The suffix to be added to the schema name.
    """

    name: str
    prefix: str = ""
    suffix: str = ""

    def get(self) -> str:
        """
        Get the schema name with prefix and suffix.
        """
        return self.prefix + self.name + self.suffix

    def __str__(self):
        return self.get()
