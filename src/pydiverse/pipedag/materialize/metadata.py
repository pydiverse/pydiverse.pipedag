from __future__ import annotations

import datetime
from dataclasses import dataclass


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
    position_hash: str
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

    Attention: `task_hash` is sometimes taken from cache and thus is not guaranteed
    to refer to the `task_hash` that corresponds to the currently executed task.
    Instead, it refers to the task that originally produced this object.
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

    The `prev_objects` and `stage` values are used to retrieve the appropriate
    tables from the cache.

    Attention: `task_hash` is sometimes taken from cache and thus is not guaranteed
    to refer to the `task_hash` that corresponds to the currently executed task.
    Instead, it refers to the task that originally produced this object.
    """

    prev_objects: list[str]
    new_objects: list[str]
    stage: str
    query_hash: str
    task_hash: str
