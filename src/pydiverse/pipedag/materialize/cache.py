from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.container import RawSql
from pydiverse.pipedag.materialize.metadata import (
    LazyTableMetadata,
    RawSqlMetadata,
)
from pydiverse.pipedag.util.hashing import stable_hash

if TYPE_CHECKING:
    from pydiverse.pipedag import Stage, Table
    from pydiverse.pipedag.backend import BaseTableStore
    from pydiverse.pipedag.materialize.core import MaterializingTask


@dataclass(frozen=True)
class TaskCacheInfo:
    task: MaterializingTask
    input_hash: str
    cache_fn_hash: str
    cache_key: str


@dataclass
class TableCacheInfo:
    # Only CacheManager class should access these members directly
    _stage: Stage
    _task_hash: str
    _query_hash: str
    _is_cache_valid: bool

    # The public interface is exposed as functions
    def is_cache_valid(self):
        return self._is_cache_valid

    def store_raw_sql_metadata(
        self, store: BaseTableStore, prev_objects: list[str], new_objects: list[str]
    ):
        # Store metadata
        # Attention: Raw SQL statements may only be executed sequentially within
        #            stage for store.get_objects_in_stage to work
        store.store_raw_sql_metadata(
            RawSqlMetadata(
                prev_objects=prev_objects,
                new_objects=new_objects,
                stage=self._stage.name,
                query_hash=self._query_hash,
                task_hash=self._task_hash,
            )
        )


class CacheManager:
    @staticmethod
    def lazy_table_cache_lookup(
        store: BaseTableStore,
        task_cache_info: TaskCacheInfo,
        table: Table,
        query_hash: str,
    ):
        task_hash = task_cache_info.cache_key
        # Store table
        try:
            # Try retrieving the table from the cache and then copying it
            # to the transaction stage
            metadata = store.retrieve_lazy_table_metadata(
                query_hash, task_hash, table.stage
            )
            store.copy_lazy_table_to_transaction(metadata, table)
            store.logger.info(f"Lazy cache of table '{table.name}' found")
            is_cache_valid = True
        except CacheError as e:
            # Either not found in cache, or copying failed
            # -> Store using default method
            store.logger.warning(
                "Cache miss", table=table.name, stage=table.stage.name, cause=str(e)
            )
            is_cache_valid = False

        # Store metadata
        store.store_lazy_table_metadata(
            LazyTableMetadata(
                name=table.name,
                stage=table.stage.name,
                query_hash=query_hash,
                task_hash=task_hash,
            )
        )
        return TableCacheInfo(table.stage, task_hash, query_hash, is_cache_valid)

    @staticmethod
    def raw_sql_cache_lookup(
        store: BaseTableStore,
        task_cache_info: TaskCacheInfo,
        raw_sql: RawSql,
        query_hash: str,
    ) -> tuple[TableCacheInfo, RawSqlMetadata | None]:
        task_hash = task_cache_info.cache_key
        # Store tables
        try:
            # Try retrieving the table from the cache and then copying it
            # to the transaction stage
            metadata = store.retrieve_raw_sql_metadata(
                query_hash, task_hash, raw_sql.stage
            )
            store.copy_raw_sql_tables_to_transaction(metadata, raw_sql.stage)
            store.logger.info(f"Lazy cache of stage '{raw_sql.stage}' found")
            is_cache_valid = True
        except CacheError as e:
            # Either not found in cache, or copying failed
            # -> Store using default method
            store.logger.warning("Cache miss for raw-SQL", cause=str(e))
            metadata = None
            is_cache_valid = False
        return (
            TableCacheInfo(raw_sql.stage, task_hash, query_hash, is_cache_valid),
            metadata,
        )

    @staticmethod
    def task_cache_key(task: MaterializingTask, input_hash: str, cache_fn_hash: str):
        """Cache key used to judge cache validity of the current task output.

        Also referred to as `task_hash`.

        For lazy objects, this hash isn't used to judge cache validity, instead it
        serves as an identifier to reference a specific task run. This can be the case
        if a task is determined to be cache-valid and the lazy query string is also
        the same, but the task_hash is different from a previous run. Then we can
        compute this combined_cache_key from the task's cache metadata to determine
        which lazy object to use as cache.

        :param task: task for which the cache key is computed
        :param input_hash: hash used for checking whether task is cache invalid due
            to changing input.
        :param cache_fn_hash: same as input_hash but for external inputs which need
            manual cache invalidation function.
        :return: The hash / cache key (str).
        """

        return stable_hash(
            "TASK",
            task.name,
            task.version,
            input_hash,
            cache_fn_hash,
        )

    @staticmethod
    def lazy_table_cache_key(task_hash: str, query_hash: str):
        return stable_hash(
            "LAZY_TABLE",
            task_hash,
            query_hash,
        )
