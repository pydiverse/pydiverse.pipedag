from __future__ import annotations

import itertools
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

from pydiverse.pipedag.util.hashing import stable_hash

if TYPE_CHECKING:
    from pydiverse.pipedag import Table
    from pydiverse.pipedag.materialize.core import MaterializingTask


class ImperativeMaterializationState:
    def __init__(self):
        # every imperatively materialized table is an assumed dependency of
        # subsequently materialized tables of the same task
        self.assumed_dependencies: set[Table] = set()
        # Table(...).materialize() returns dematerialized objects. We need to find the
        # corresponding Table objects for handing returned objects over to consumer
        # tasks.
        self.object_lookup: dict[int, Table] = {}
        self.table_ids: set[int] = set()
        self.auto_suffix_counter = itertools.count()

    def add_table_lookup(self, obj, table: Table):
        self.assumed_dependencies.add(table)
        self.object_lookup[id(obj)] = table
        self.table_ids.add(id(table))


@dataclass(frozen=True)
class TaskCacheInfo:
    task: MaterializingTask
    input_hash: str
    cache_fn_hash: str
    cache_key: str
    assert_no_materialization: bool
    force_task_execution: bool

    @cached_property
    def imperative_materialization_state(self):
        """State used by Table.materialize()"""
        return ImperativeMaterializationState()


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
