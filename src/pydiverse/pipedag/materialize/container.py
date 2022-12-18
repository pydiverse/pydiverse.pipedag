from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Generic

from pydiverse.pipedag._typing import T
from pydiverse.pipedag.util import normalize_name

if TYPE_CHECKING:
    from pydiverse.pipedag.core.stage import Stage


class Table(Generic[T]):
    """Table container

    Used to wrap table objects that get returned from materializing
    tasks.

    :param obj: The table object to wrap
    :param name: Optional name. If no name is provided, an automatically
        generated name will be used. To prevent name collisions, you can
        add '%%' at the end of the name to enable automatic name mangling.
    :param stage: Stage in which the result was emitted
    :param primary_key: Optional name of the primary key that should be
        used when materializing this table
    :param store_id: Optional key used for cache invalidation (manual use is discouraged)
    """

    def __init__(
        self,
        obj: T | None = None,
        name: str | None = None,
        stage: Stage | None = None,
        primary_key: str | None = None,
        store_id: str | None = None,
    ):
        self._name = None

        self.obj = obj
        self.name = name
        self.stage = stage
        self.primary_key = primary_key

        # This store_id is used for downstream cache invalidation in case this table is not retrieved from cache
        self.store_id = store_id or uuid.uuid4().hex[:20]

    def __str__(self):
        return f"<Table: {self.name} ({self.stage.name})>"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if value is not None and not isinstance(value, str):
            raise TypeError(f"Table name must be of instance 'str' not {type(value)}.")
        self._name = normalize_name(value)


class RawSql:
    """Container for raw sql strings

    This allows wrapping legacy sql code with pipedag before it is converted to proper tasks that allow tracing tables.

    :param sql: The table object to wrap
    :param name: A name that allows humans to reference the sql
    :param stage: Stage in which the result was emitted
    :param store_id: Optional ID used for cache invalidation (manual use is discouraged)
    """

    def __init__(
        self,
        sql: str | None = None,
        name: str | None = None,
        stage: Stage | None = None,
        store_id: str | None = None,
    ):
        self._name = None

        self.sql = sql
        self.name = name
        self.stage = stage

        # This store_id is used for downstream cache invalidation in case this table is not retrieved from cache
        self.store_id = store_id or uuid.uuid4().hex[:20]

    def __str__(self):
        sql_short = self.sql.strip()[0:40].replace("\n", "").strip()
        return f"<Raw SQL: {self.name}/{self.stage}:{sql_short}>"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if value is not None and not isinstance(value, str):
            raise TypeError(f"Table name must be of instance 'str' not {type(value)}.")
        self._name = normalize_name(value)


class Blob(Generic[T]):
    """Blob (binary large object) container

    Used to wrap arbitrary python objects that get returned from materializing
    tasks.

    :param obj: The object to wrap
    :param name: Optional name. If no name is provided, an automatically
        generated name will be used. To prevent name collisions, you can
        add '%%' at the end of the name to enable automatic name mangling.
    :param stage: Stage in which the result was emitted
    :param store_id: Optional key used for cache invalidation (manual use is discouraged)
    """

    def __init__(
        self,
        obj: T | None = None,
        name: str | None = None,
        stage: Stage | None = None,
        store_id: str | None = None,
    ):
        self._name = None

        self.obj = obj
        self.name = name
        self.stage = stage

        self.store_id = store_id

    def __str__(self):
        return f"<Blob: {self.name} ({self.stage.name})>"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = normalize_name(value)
