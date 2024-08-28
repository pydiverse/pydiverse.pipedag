from __future__ import annotations

from pathlib import Path

import filelock
import pandas as pd
import pytest
import sqlalchemy as sa

from pydiverse.pipedag import ConfigContext, Flow, RawSql, Stage, Table, materialize
from pydiverse.pipedag.backend.table.sql.ddl import (
    CreateSchema,
    CreateTableAsSelect,
    DropTable,
)
from pydiverse.pipedag.container import ExternalTableReference, Schema
from tests.fixtures.instances import with_instances
from tests.util.sql import sql_table_expr
from tests.util.tasks_library import (
    assert_table_equal,
    noop_sql,
    simple_dataframe,
    simple_lazy_table,
)


@with_instances("ibm_db2")
def test_db2_nicknames():
    lock_path = Path(__file__).parent / "scripts" / "lock"

    @materialize(input_type=sa.Table)
    def create_nicknames(table: sa.sql.expression.Alias):
        script_path = Path(__file__).parent / "scripts" / "simple_nicknames.sql"
        simple_nicknames = Path(script_path).read_text(encoding="utf-8")
        simple_nicknames = simple_nicknames.replace(
            "{{out_schema}}", str(table.original.schema)
        )
        simple_nicknames = simple_nicknames.replace(
            "{{out_table}}", str(table.original.name)
        )

        return RawSql(simple_nicknames, "create_nicknames", separator="|")

    with Flow("f") as f:
        with Stage("stage"):
            x = simple_dataframe()
            nicknames = create_nicknames(x)
            _ = nicknames

    with filelock.FileLock(lock_path):
        # We run three times to ensure that the nicknames created in the first run
        # have to be dropped, since the same schema is reused.
        assert f.run().successful
        assert f.run().successful
        assert f.run().successful


@with_instances("ibm_db2")  # only one instance to avoid parallel DRDA WRAPPER creation
def test_db2_table_reference_nicknames():
    lock_path = Path(__file__).parent / "scripts" / "lock"

    @materialize(nout=2)
    def create_external_nicknames():
        table_store = ConfigContext.get().store.table_store
        schema = Schema("user_controlled_schema", prefix="", suffix="")
        table_name = "external_table_for_nickname"
        table_store.execute(CreateSchema(schema, if_not_exists=True))
        table_store.execute(DropTable(table_name, schema, if_exists=True))
        query = sql_table_expr({"col": [0, 1, 2, 3]})
        table_store.execute(
            CreateTableAsSelect(
                table_name,
                schema,
                query,
            )
        )
        script_path = Path(__file__).parent / "scripts" / "simple_nicknames.sql"
        simple_nicknames = Path(script_path).read_text()
        simple_nicknames = simple_nicknames.replace("{{out_schema}}", schema.get())
        simple_nicknames = simple_nicknames.replace("{{out_table}}", table_name)

        table_store.execute_raw_sql(
            RawSql(simple_nicknames, "create_external_nicknames", separator="|")
        )

        return Table(
            ExternalTableReference(
                "nick1", schema=schema.get(), shared_lock_allowed=False
            )
        ), Table(
            ExternalTableReference(
                "nick2", schema=schema.get(), shared_lock_allowed=True
            )
        )

    with Flow("f") as f:
        with Stage("stage"):
            nick_1_ref, nick_2_ref = create_external_nicknames()
            nick_1_ref_noop = noop_sql(nick_1_ref)
            nick_2_ref_noop = noop_sql(nick_2_ref)
            assert_table_equal(nick_1_ref, nick_1_ref_noop)
            assert_table_equal(nick_2_ref, nick_2_ref_noop)

    with filelock.FileLock(lock_path):
        # We run three times to ensure that the nicknames created in the first run
        # have to be dropped, since the same schema is reused.
        assert f.run().successful
        assert f.run().successful
        assert f.run().successful


@with_instances("ibm_db2_materialization_details")
@pytest.mark.parametrize("task", [simple_dataframe, simple_lazy_table])
def test_db2_table_spaces(task):
    @materialize()
    def create_table_spaces():
        script_path = Path(__file__).parent / "scripts" / "simple_table_spaces.sql"
        simple_table_spaces = Path(script_path).read_text(encoding="utf-8")
        return RawSql(simple_table_spaces, "create_table_spaces", separator="|")

    @materialize(input_type=sa.Table, lazy=False)
    def get_actual_table_space_attributes(table: sa.sql.expression.Alias):
        query = f"""
            SELECT TBSPACE, INDEX_TBSPACE, LONG_TBSPACE FROM SYSCAT.TABLES
             WHERE TABSCHEMA = '{table.original.schema.upper()}'
             AND TABNAME = '{table.original.name.upper()}'
        """
        return Table(sa.text(query), f"tbspace_attributes_{table.name}")

    @materialize(version="1.0")
    def get_expected_table_space_attributes():
        return Table(
            pd.DataFrame(
                {"TBSPACE": ["S1"], "INDEX_TBSPACE": ["S2"], "LONG_TBSPACE": ["S3"]}
            ),
            name="tbspace_attributes",
        )

    with Flow("f") as f:
        with Stage("stage", materialization_details="table_space"):
            create_table_spaces()
            x = task()
            x_attr = get_actual_table_space_attributes(x)
            attrs = get_expected_table_space_attributes()
            assert_table_equal(attrs, x_attr)

    for _ in range(3):
        assert f.run().successful
