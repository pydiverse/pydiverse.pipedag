from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from pydiverse.pipedag._typing import T
from pydiverse.pipedag.util import normalise_name

if TYPE_CHECKING:
    from pydiverse.pipedag.core.schema import Schema


class Table(Generic[T]):
    """Table container

    Used to wrap table objects that get returned from materialising
    tasks.

    :param obj: The table object to wrap
    :param name: Optional name. If no name is provided, an automatically
        generated name will be used. To prevent name collisions, you can
        add '%%' at the end of the name to enable automatic name mangling.
    :param primary_key: Optional name of the primary key that should be
        used when materialising this table
    """

    def __init__(
        self,
        obj: T = None,
        name: str = None,
        schema: Schema = None,
        primary_key: str = None,
        cache_key: str = None,
    ):
        self._name = None

        self.obj = obj
        self.name = name
        self.schema = schema
        self.primary_key = primary_key

        self.cache_key = cache_key

    def __str__(self):
        return f"<Table: {self.name} ({self.schema.name})>"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = normalise_name(value)


class Blob:
    """Blob (binary large object) container

    Used to wrap arbitrary python objects that get returned from materialising
    tasks.

    :param obj: The object to wrap
    :param name: Optional name. If no name is provided, an automatically
        generated name will be used. To prevent name collisions, you can
        add '%%' at the end of the name to enable automatic name mangling.
    """

    def __init__(
        self,
        obj: Any = None,
        name: str = None,
        schema: Schema = None,
        cache_key: str = None,
    ):
        self._name = None

        self.obj = obj
        self.name = name
        self.schema = schema

        self.cache_key = cache_key

    def __str__(self):
        return f"<Blob: {self.name} ({self.schema.name})>"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = normalise_name(value)
