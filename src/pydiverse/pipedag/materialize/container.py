from __future__ import annotations

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
    :param primary_key: Optional name of the primary key that should be
        used when materializing this table
    """

    def __init__(
        self,
        obj: T = None,
        name: str = None,
        stage: Stage = None,
        primary_key: str = None,
        cache_key: str = None,
    ):
        self._name = None

        self.obj = obj
        self.name = name
        self.stage = stage
        self.primary_key = primary_key

        self.cache_key = cache_key

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


class Blob(Generic[T]):
    """Blob (binary large object) container

    Used to wrap arbitrary python objects that get returned from materializing
    tasks.

    :param obj: The object to wrap
    :param name: Optional name. If no name is provided, an automatically
        generated name will be used. To prevent name collisions, you can
        add '%%' at the end of the name to enable automatic name mangling.
    """

    def __init__(
        self,
        obj: T = None,
        name: str = None,
        stage: Stage = None,
        cache_key: str = None,
    ):
        self._name = None

        self.obj = obj
        self.name = name
        self.stage = stage

        self.cache_key = cache_key

    def __str__(self):
        return f"<Blob: {self.name} ({self.stage.name})>"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = normalize_name(value)
