from typing import TYPE_CHECKING, Any, Generic

from pdpipedag._typing import T

if TYPE_CHECKING:
    from pdpipedag.core.schema import Schema


class Table(Generic[T]):

    def __init__(
            self,
            obj: T = None,
            name: str = None,
            schema: 'Schema' = None,
            primary_key: str = None,
            cache_key: str = None,
    ):
        self.obj = obj
        self.name = name
        self.schema = schema
        self.primary_key = primary_key

        self.cache_key = cache_key

    def __str__(self):
        return f'<Table: {self.name} ({self.schema.name})>'


class Blob:

    def __init__(
            self,
            obj: Any = None,
            name: str = None,
            schema: 'Schema' = None,
            cache_key: str = None,
    ):
        self.obj = obj
        self.name = name
        self.schema = schema

        self.cache_key = cache_key

    def __str__(self):
        return f'<Blob: {self.name} ({self.schema.name})>'
