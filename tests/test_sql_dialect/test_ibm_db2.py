from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa

from pydiverse.pipedag import Flow, RawSql, Stage, materialize
from tests.fixtures.instances import with_instances
from tests.util.tasks_library import simple_dataframe


@with_instances("ibm_db2", "ibm_db2_avoid_schema")
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

    with Flow("nick_flow") as f:
        with Stage("stage"):
            x = simple_dataframe()
            nicknames = create_nicknames(x)
            _ = nicknames

    # We run three times to ensure that the nicknames created in the first run
    # have to be dropped, since the same schema is reused.
    assert f.run().successful
    assert f.run().successful
    assert f.run().successful
