from __future__ import annotations

import random

import structlog

from pydiverse.pipedag import ConfigContext, Table
from pydiverse.pipedag.backend import SQLTableStore
from pydiverse.pipedag.backend.table.sql.ddl import DropTable
from pydiverse.pipedag.context import TaskContext
from pydiverse.pipedag.materialize.core import MaterializingTask
from pydiverse.pipedag.materialize.store import mangle_table_name
from pydiverse.pipedag.util.hashing import stable_hash


def materialize_table(
    table: Table,
    debug_suffix: str | None = None,
    flag_task_debug_tainted: bool = True,
    keep_table_name: bool = True,
    drop_if_exists: bool = True,
):
    """
    This function allows the user to materialize a table ad-hoc
    whenever the TaskContext is defined.
    This can be useful during interactive debugging.
    If table.name is not given, then it is derived from the task name and a suffix.
    The suffix is either the debug_suffix (if given) or a random suffix.
    If the table name ends in %%, the %% are also replaced by a suffix.

    :param table: The table to be materialized.
    :param debug_suffix: A suffix to be appended to the table name
        for debugging purposes. Default: None
    :param flag_task_debug_tainted: Whether to flag the task as tainted
        by this debug materialization. The flag will cause an exception if execution is
        continued. Default: True
    :param keep_table_name: if False, the table.name will be equal to the debug name
        after return. Otherwise, the original name of the table is preserved.
        Default: True
    :param drop_if_exists: If True, try to drop the table (if exists) before recreating.
        This is only supported for SQL table stores.
    """
    config_context = ConfigContext.get()
    table_store = config_context.store.table_store

    task_context = TaskContext.get()
    task: MaterializingTask = task_context.task  # type: ignore

    table.stage = task.stage

    suffix = (
        stable_hash(str(random.randbytes(8))) + "_0000" if debug_suffix is None else ""
    )
    old_table_name = table.name
    table.name = mangle_table_name(table.name, task.name, suffix)
    if debug_suffix is not None:
        table.name += debug_suffix

    if flag_task_debug_tainted:
        task.debug_tainted = True

    if drop_if_exists:
        if isinstance(table_store, SQLTableStore):
            schema = table_store.get_schema(task.stage.transaction_name)
            table_store.execute(DropTable(table.name, schema, if_exists=True))
        else:
            logger = structlog.get_logger(logger_name="Debug materialize_table")
            logger.warning(
                "drop_if_exists not supported for non SQLTableStore table stores."
            )
    table_store.store_table(table, task)

    new_table_name = table.name
    if keep_table_name:
        table.name = old_table_name
    return new_table_name
