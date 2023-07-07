from __future__ import annotations

from abc import ABC, abstractmethod

import structlog

from pydiverse.pipedag import Stage
from pydiverse.pipedag._typing import T
from pydiverse.pipedag.backend.table.base import TableHookResolver
from pydiverse.pipedag.materialize.container import Table
from pydiverse.pipedag.materialize.core import MaterializingTask, TaskInfo
from pydiverse.pipedag.util import Disposable


class BaseTableCache(ABC, TableHookResolver, Disposable):
    def __init__(
        self,
        store_input: bool = True,
        store_output: bool = False,
        use_stored_input_as_cache: bool = True,
    ):
        super().__init__()

        self.logger = structlog.get_logger(logger_name=type(self).__name__)

        self.should_store_input = store_input
        self.should_store_output = store_output
        self.should_use_stored_input_as_cache = use_stored_input_as_cache

    def setup(self):
        """Setup function

        This function gets called at the beginning of a flow run.
        Unlike the __init__ method, a lock is acquired before
        the setup method gets called to prevent race conditions.
        """

    def init_stage(self, stage: Stage):
        """Initialize a stage

        Gets called before any table is attempted to be stored in the stage.
        """

    @abstractmethod
    def clear_cache(self, stage: Stage):
        """Delete the cache for a specific stage"""

    def store_table(self, table: Table, task: MaterializingTask, task_info: TaskInfo):
        if self.should_store_output:
            return self._store_table(table, task, task_info)

    def store_input(self, table: Table, task: MaterializingTask, task_info: TaskInfo):
        if self.should_store_input:
            return self._store_table(table, task, task_info)

    def _store_table(
        self,
        table: Table,
        task: MaterializingTask | None,
        task_info: TaskInfo | None,
    ):
        try:
            hook = self.get_m_table_hook(type(table.obj))
            hook.materialize(self, table, table.stage.transaction_name, task_info)
        except TypeError:
            return None

    def retrieve_table_obj(self, table: Table, as_type: type[T]) -> T:
        if not self.should_use_stored_input_as_cache:
            return None
        if not self._has_table(table, as_type):
            return None
        return self._retrieve_table_obj(table, as_type)

    def _retrieve_table_obj(self, table: Table, as_type: type[T]) -> T:
        try:
            hook = self.get_r_table_hook(as_type)
            obj = hook.retrieve(self, table, table.stage.name, as_type)
            self.logger.info("Retrieved table from local table cache", table=table)
            return obj
        except Exception as e:
            self.logger.info(
                "Failed to retrieve table from local table cache",
                table=table,
                cause=str(e),
            )
            return None

    @abstractmethod
    def _has_table(self, table: Table, as_type: type) -> bool:
        """Check if the given table is in the cache"""
