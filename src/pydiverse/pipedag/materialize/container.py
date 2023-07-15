from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any, Generic

from pydiverse.pipedag._typing import T
from pydiverse.pipedag.util import normalize_name

if TYPE_CHECKING:
    from pydiverse.pipedag.core.stage import Stage


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
    :param name: Optional name. If no name is provided, an automatically
        generated name will be used. To prevent name collisions, you can
        append ``"%%"`` at the end of the name to enable automatic name mangling.
    :param primary_key: Optional name of the primary key that should be
        used when materializing this table. Only supported by some table stores.
    :param indexes: Optional list of indexes to create. Each provided index should be
        a list of column names. Only supported by some table stores.
    :param type_map: Optional map of column names to types. Depending on the table
        store this will allow you to control the datatype as which the specified
        columns get materialized.

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
    ):
        self._name = None
        self.stage: Stage | None = None

        self.obj = obj
        self.name = name
        self.primary_key = primary_key
        self.indexes = indexes
        self.type_map = type_map

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

        # cache_key will be overridden shortly before handing over to downstream tasks
        # that use it to compute their input_hash for cache_invalidation due to input
        # change
        self.cache_key = None

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

    def __getstate__(self):
        # The table `obj` field can't necessarily be pickled. That's why we remove it
        # from the state before pickling.
        state = self.__dict__.copy()
        state["obj"] = None
        return state


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

    :param sql: The sql query string to execute. Depending on the database dialect,
        the query will be split into multiple subqueries that then get
        executed sequentially.
    :param name: Optional name. If no name is provided, an automatically
        generated name will be used. To prevent name collisions, you can
        append ``"%%"`` at the end of the name to enable automatic name mangling.
    """

    def __init__(
        self,
        sql: str | None = None,
        name: str | None = None,
    ):
        self._name = None
        self.stage: Stage | None = None

        self.sql = sql
        self.name = name

        # cache_key will be overridden shortly before handing over to downstream tasks
        # that use it to compute their input_hash for cache_invalidation due to input
        # change
        self.cache_key = None

    def __repr__(self):
        stage_name = self.stage.name if self.stage else None
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
    :param name: Optional name. If no name is provided, an automatically
        generated name will be used. To prevent name collisions, you can
        append ``"%%"`` at the end of the name to enable automatic name mangling.

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
