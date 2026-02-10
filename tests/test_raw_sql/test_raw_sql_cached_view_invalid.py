# Copyright (c) QuantCo and pydiverse contributors 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

import sqlalchemy as sa

from pydiverse.pipedag import CacheValidationMode, Flow, Schema, Stage, materialize
from pydiverse.pipedag.backend.table.sql.ddl import CreateSchema, DropSchema
from pydiverse.pipedag.container import RawSql
from pydiverse.pipedag.context import ConfigContext, TaskContext
from tests.fixtures.instances import with_instances
from tests.test_raw_sql.util import raw_sql_bind_schema

EXT_SCHEMA = Schema("external_schema", prefix="", suffix="")


@with_instances("mssql")
def test_raw_sql_cached_view_becomes_invalid():
    """When a cached view becomes invalid because an external table was
    modified, the flow should still succeed when re-run with an updated task.

    Scenario:
    1. An external table ``external_schema.t(a, b)`` exists outside pipedag.
    2. A raw SQL task creates a view selecting columns a, b from that table.
    3. Column b is dropped from the external table, making the view
       definition invalid.
    4. The raw SQL task is updated (new view selecting only a) and the flow
       is re-run.  During the commit phase, the schema swap must handle the
       old invalid view in the committed schema.
    """

    @materialize(input_type=sa.Table, lazy=True)
    def raw_sql(view_sql):
        """Raw SQL task that creates a view from an inline SQL string."""
        stage = TaskContext.get().task._stage
        sql = raw_sql_bind_schema(view_sql, "out_", stage, transaction=True)
        return RawSql(sql)

    config = ConfigContext.get()
    store = config.store.table_store

    # (Re-)create external schema and table with columns (a, b)
    store.execute(DropSchema(EXT_SCHEMA, if_exists=True, cascade=True, engine=store.engine))
    store.execute(CreateSchema(EXT_SCHEMA))
    with store.engine.connect() as conn:
        conn.execute(sa.text("SELECT 1 as a, 2 as b INTO [external_schema].[t]"))
        if sa.__version__ >= "2.0.0":
            conn.commit()

    # --- First run: view selects a, b from external table ---
    sql_v1 = "CREATE VIEW {{out_schema}}.v AS SELECT a, b FROM [external_schema].[t];\nGO\n"
    with Flow() as f:
        with Stage("raw"):
            raw_sql(sql_v1)

    result = f.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
    assert result.successful

    # Drop column b from the external table.
    # On MSSQL the dependent view still exists but its definition is now
    # invalid (it references column b which no longer exists).
    with store.engine.connect() as conn:
        conn.execute(sa.text("ALTER TABLE [external_schema].[t] DROP COLUMN [b]"))
        if sa.__version__ >= "2.0.0":
            conn.commit()

    # --- Second run: updated task with new SQL selecting only column a ---
    sql_v2 = "CREATE VIEW {{out_schema}}.v AS SELECT a FROM [external_schema].[t];\nGO\n"
    with Flow() as f:
        with Stage("raw"):
            raw_sql(sql_v2)

    # Fall back to dropping the old schema instead of doing the schema swap.
    result = f.run()
    assert result.successful
