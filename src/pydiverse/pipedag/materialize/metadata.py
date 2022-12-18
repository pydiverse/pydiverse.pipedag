from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import List


@dataclass
class TaskMetadata:
    """Metadata associated with a task

    This metadata object contains all the necessary information that is
    needed for determining if a task has already been executed with the
    same inputs, and all the information that is needed to reconstruct
    the output.
    """

    name: str
    stage: str
    version: str | None
    timestamp: datetime.datetime
    run_id: str
    input_hash: str  # input cache validity in DAG
    cache_fn_hash: str  # manually managed source input cache validity
    output_json: str


@dataclass
class LazyTableMetadata:
    """Metadata associated with a 'lazy table'

    This class is only provided for convenience for those table store
    backends that implement the `lazy` option for the `store_table` method.

    The `query_hash` is initially filled with the effective task cache invalidation hash
    which depends on Flow.run(ignore_fresh_input=) parameter, task.input_hash,
    task.version, and task.cache_fn_hash. In a second step, the lazy query string
    is also factored in the cache key, so lazy tables are automatically cache managed.

    The `name` and `stage` values are used to retrieve the appropriate
    table from the cache.

    The `store_id` is effectively just a UUID which distingishes two table materializations when
    following tasks read the metadata as their input.
    """

    name: str
    stage: str
    query_hash: str
    store_id: str


@dataclass
class RawSqlMetadata:
    """Metadata associated with raw sql statements

    The query_hash is initially filled with the effective task cache invalidation hash
    which depends on Flow.run(ignore_fresh_input=) parameter, task.input_hash,
    task.version, and task.cache_fn_hash. In a second step, the lazy query string
    is also factored in the cache key, so lazy tables are automatically cache managed.

    The `prev_tables`, `tables` and `stage` values are used to retrieve the appropriate
    tables from the cache.
    """

    prev_tables: list[str]
    tables: list[str]
    stage: str
    query_hash: str
    store_id: str
