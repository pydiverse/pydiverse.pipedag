from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydiverse.pipedag.util.hashing import stable_hash

if TYPE_CHECKING:
    from pydiverse.pipedag.materialize.core import MaterializingTask


@dataclass(frozen=True)
class TaskCacheInfo:
    task: MaterializingTask
    input_hash: str
    cache_fn_hash: str
    cache_key: str


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


def lazy_table_cache_key(task_hash: str, query_hash: str):
    return stable_hash(
        "LAZY_TABLE",
        task_hash,
        query_hash,
    )
