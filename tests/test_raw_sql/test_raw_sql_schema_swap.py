from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, materialize
from pydiverse.pipedag.context import ConfigContext, TaskContext
from pydiverse.pipedag.materialize.container import RawSql
from tests.fixtures.instances import with_instances


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
    sql = Path(script_path).read_text()
    sql = raw_sql_bind_schema(sql, "out_", stage, transaction=True)
    sql = raw_sql_bind_schema(sql, "in_", input_stage)
    return RawSql(sql, Path(script_path).name, stage)


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


@with_instances("mssql")
def test_raw_sql_schema_swap():
    # This test creates various different objects in one schema and then
    # checks if, after swapping the schema, if they are still working correctly.
    # TODO: Extend tests for other backends

    instance_name = ConfigContext.get().instance_name
    dir_ = Path(__file__).parent / "scripts" / instance_name / "schema_swap"

    with Flow() as f:
        with Stage("raw_0") as raw_0:
            sql_script("create_objects.sql", dir_)
        with Stage("raw_1"):
            sql_script("check_objects.sql", dir_, input_stage=raw_0)

    f.run()
    f.run()
