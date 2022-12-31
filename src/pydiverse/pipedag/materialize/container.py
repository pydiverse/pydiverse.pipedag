from __future__ import annotations

from typing import TYPE_CHECKING, Generic

from pydiverse.pipedag._typing import T
from pydiverse.pipedag.materialize.util import compute_cache_key
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
    :param primary_key: Optional name of the primary key that should be
        used when materializing this table
    :param cache_key: Internal cache_key used when retreiving an object
        from the database cache.
    """

    def __init__(
        self,
        obj: T | None = None,
        name: str | None = None,
        stage: Stage | None = None,
        primary_key: str | None = None,
        *,
        cache_key: str | None = None,
    ):
        self._name = None

        self.obj = obj
        self.name = name
        self.stage = stage
        self.primary_key = primary_key

        self.cache_info = CacheInfo(cache_key)

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

    This allows wrapping legacy sql code with pipedag before it is converted
    to proper tasks that allow tracing tables.

    :param sql: The table object to wrap
    :param cache_key: Internal cache_key used when retreiving an object
        from the database cache.
    """

    def __init__(
        self,
        sql: str | None = None,
        name: str | None = None,
        stage: Stage | None = None,
        *,
        cache_key: str | None = None,
    ):
        self._name = None

        self.sql = sql
        self.name = name
        self.stage = stage

        self.cache_info = CacheInfo(cache_key)

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
    :param cache_key: Internal cache_key used when retreiving an object
        from the database cache.
    """

    def __init__(
        self,
        obj: T | None = None,
        name: str | None = None,
        stage: Stage | None = None,
        *,
        cache_key: str | None = None,
    ):
        self._name = None

        self.obj = obj
        self.name = name
        self.stage = stage

        self.cache_info = CacheInfo(cache_key)

    def __str__(self):
        return f"<Blob: {self.name} ({self.stage.name})>"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = normalize_name(value)


class CacheInfo:
    """Object that stores different pieces of cache information

    If `cache_key` is set manually or `deserialized_cache_key` is set during
    initialization, this cache key will be used. Otherwise, the `cache_key` parameter
    will be the combination of all provided pieces of cache information.
    """

    def __init__(self, deserialized_cache_key: str = None):
        self.__cache_key = deserialized_cache_key

        self.task_hash = None
        self.query_hash = None

    def __repr__(self):
        return f"<CacheInfo: task_hash={self.task_hash} query_hash={self.query_hash}>"

    @property
    def cache_key(self):
        if self.__cache_key is not None:
            return self.__cache_key

        return compute_cache_key(
            self.task_hash,
            self.query_hash,
        )

    @cache_key.setter
    def cache_key(self, value):
        self.__cache_key = value
