from __future__ import annotations

import random

import structlog

from pydiverse.pipedag import ConfigContext, Table
from pydiverse.pipedag.backend import SQLTableStore
from pydiverse.pipedag.backend.table.sql.ddl import DropTable
from pydiverse.pipedag.container import Schema
from pydiverse.pipedag.context import TaskContext
from pydiverse.pipedag.materialize.core import MaterializingTask
from pydiverse.pipedag.materialize.store import mangle_table_name
from pydiverse.pipedag.util.hashing import stable_hash


def materialize_table(
    table: Table,
    config_context: ConfigContext | None = None,
    schema: Schema | str | None = None,
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
    :param config_context: The config context to be used for materialization.
        If None, the current config context is used. Default: None
    :param schema: The schema to be used for writing table. If None, the
        schema is derived from the table's stage. Default: None
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
    if config_context is None:
        config_context = ConfigContext.get()
    table_store = config_context.store.table_store

    task: MaterializingTask | None = None
    try:
        task_context = TaskContext.get()
        task = task_context.task  # type: ignore
        task_name = task.name

        table.stage = task.stage
        if schema is None:
            schema = table_store.get_schema(table.stage.transaction_name)
    except LookupError:
        task_name = None
        # LookupError happens if no TaskContext is open
        if schema is None:
            raise ValueError(
                "Parameter schema must be provided if task is not called by "
                "normal pipedag orchestration."
            ) from None

    suffix = (
        stable_hash(str(random.randbytes(8))) + "_0000" if debug_suffix is None else ""
    )
    old_table_name = table.name
    table.name = mangle_table_name(table.name, task_name, suffix)
    if debug_suffix is not None:
        table.name += debug_suffix

    if flag_task_debug_tainted and task is not None:
        task.debug_tainted = True

    if drop_if_exists:
        if isinstance(table_store, SQLTableStore):
            table_store.execute(DropTable(table.name, schema, if_exists=True))
        else:
            logger = structlog.get_logger(logger_name="Debug materialize_table")
            logger.warning(
                "drop_if_exists not supported for non SQLTableStore table stores."
            )
    if task is None:
        hook = table_store.get_m_table_hook(type(table.obj))
        schema_name = (
            schema.name
            if table_store.get_schema(schema.name).get() == schema.get()
            else schema.get()
        )
        if table_store.get_schema(schema_name).get() != schema.get():
            raise ValueError(
                "Schema prefix and postfix must match prefix and postfix of provided "
                "config_context: "
                f"{table_store.get_schema(schema_name).get()} != {schema.get()}"
            )
        hook.materialize(table_store, table, schema_name)
    else:
        table_store.store_table(table, task)

    new_table_name = table.name
    if keep_table_name:
        table.name = old_table_name
    return new_table_name
