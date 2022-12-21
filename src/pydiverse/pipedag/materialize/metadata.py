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
    input_hash: str
    cache_fn_hash: str
    output_json: str


@dataclass
class LazyTableMetadata:
    """Metadata associated with a 'lazy table'

    This class is only provided for convenience for those table store
    backends that implement the `lazy` option for the `store_table` method.

    The `query_hash` is a hash of the query string that produced this table.
    The `task_hash` is the combined hash of the task that produced this table.

    The `name` and `stage` values are used to retrieve the appropriate
    table from the cache.

    Attention: `task_hash` is sometimes recovered from cache and is not guaranteed to be a hash
    of the current input_hash, version, and cache_fn_hash situation of this task execution. It
    rather serves as a unique ID for what is currently stored in the task output.
    """

    name: str
    stage: str
    query_hash: str
    task_hash: str


@dataclass
class RawSqlMetadata:
    """Metadata associated with raw sql statements

    The `query_hash` is a hash of the raw sql string.
    The `task_hash` is the combined hash of the task that produced statement.

    The `tables` and `stage` values are used to retrieve the appropriate
    tables from the cache.

    Attention: `task_hash` is sometimes recovered from cache and is not guaranteed to be a hash
    of the current input_hash, version, and cache_fn_hash situation of this task execution. It
    rather serves as a unique ID for what is currently stored in the task output.
    """

    prev_tables: list[str]
    tables: list[str]
    stage: str
    query_hash: str
    task_hash: str
