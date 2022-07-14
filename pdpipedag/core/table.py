from typing import Generic

from pdpipedag._typing import T
from pdpipedag.core import schema


class Table(Generic[T]):

    def __init__(
            self,
            obj: T = None,
            name: str = None,
            schema: 'schema.Schema' = None,
            primary_key: str = None,
    ):
        self.obj = obj
        self.name = name
        self.schema = schema
        self.primary_key = primary_key
