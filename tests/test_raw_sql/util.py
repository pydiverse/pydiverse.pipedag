from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa

from pydiverse.pipedag import Stage, materialize
from pydiverse.pipedag.container import RawSql
from pydiverse.pipedag.context import ConfigContext, TaskContext


@materialize(input_type=sa.Table, lazy=True)
def sql_script(
    name: str,
    script_directory: Path,
    *,
    input_stage=None,
    depend=None,
):
    _ = depend  # only relevant for adding additional task dependency
    stage = TaskContext.get().task.stage

    script_path = script_directory / name
    sql = Path(script_path).read_text(encoding="utf-8")
    sql = raw_sql_bind_schema(sql, "out_", stage, transaction=True)
    sql = raw_sql_bind_schema(sql, "in_", input_stage)
    return RawSql(sql)


def raw_sql_bind_schema(
    sql, prefix: str, stage: Stage | RawSql | None, *, transaction=False
):
    config = ConfigContext.get()
    store = config.store.table_store
    if stage is not None:
        stage_name = stage.transaction_name if transaction else stage.name
        schema_name = store.get_schema(stage_name).get()
        sql = sql.replace(f"{{{{{prefix}schema}}}}", schema_name)
    return sql
