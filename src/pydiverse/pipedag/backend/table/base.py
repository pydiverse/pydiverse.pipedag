from __future__ import annotations

from abc import ABC, abstractmethod

from pydiverse.pipedag._typing import T
from pydiverse.pipedag.backend.metadata import TaskMetadata
from pydiverse.pipedag.core import MaterialisingTask, Schema, Table


class BaseTableStore(ABC):
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

    def setup(self):
        """Setup function

        This function gets called by the PipeDAGStore when it gets
        initialised. Unlike the __init__ method, a lock is acquired before
        the setup method gets called to prevent race conditions.
        """

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
