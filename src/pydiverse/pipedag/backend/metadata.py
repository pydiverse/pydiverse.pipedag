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
    cache_key: str
    output_json: str


@dataclass
class LazyTableMetadata:
    """Metadata associated with a 'lazy table'

    This class is only provided for convenience for those table store
    backends that implement the `lazy` option for the `store_table` method.

    The `cache_key` should incorporate the `cache_key` value of the
    producing task (this ensures that there will be no match if the inputs
    to the task change) and the query that produces the table.

    The `name` and `stage` values are used to retrieve the appropriate
    table from the cache.
    """

    name: str
    stage: str
    cache_key: str
