from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import sqlalchemy as sa

from pydiverse.pipedag import Flow, RawSql, Stage, Table, materialize
from tests.fixtures.instances import with_instances
from tests.util.tasks_library import (
    assert_table_equal,
    simple_dataframe,
    simple_lazy_table,
)


@with_instances("ibm_db2", "ibm_db2_avoid_schema", "ibm_db2_materialization_details")
def test_db2_nicknames():
    @materialize(input_type=sa.Table)
    def create_nicknames(table: sa.Table):
        script_path = Path(__file__).parent / "scripts" / "simple_nicknames.sql"
        simple_nicknames = Path(script_path).read_text()
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
        simple_table_spaces = Path(script_path).read_text()
        return RawSql(simple_table_spaces, "create_table_spaces", separator="|")

    @materialize(input_type=sa.Table, lazy=False)
    def get_actual_table_space_attributes(table: sa.Table):
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
