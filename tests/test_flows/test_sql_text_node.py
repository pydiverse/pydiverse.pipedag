from __future__ import annotations

from pathlib import Path

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import ConfigContext, Flow, Stage, Table, materialize
from tests.fixtures.instances import with_instances


@materialize(input_type=sa.Table, lazy=True)
def table_1(script_path: str):
    sql = Path(script_path).read_text(encoding="utf-8")
    return Table(sa.text(sql), name="table_1")


@materialize(input_type=sa.Table, lazy=True)
def table_2(script_path: str, dependent_table: Table):
    sql = (
        Path(script_path)
        .read_text(encoding="utf-8")
        .replace("{{dependent}}", str(dependent_table.original))
    )
    return Table(sa.text(sql), name="test_table2")


@materialize(input_type=pd.DataFrame, lazy=True)
def assert_result(df: pd.DataFrame):
    pd.testing.assert_frame_equal(
        df, pd.DataFrame({"coltab2": [24]}), check_dtype=False
    )


@with_instances("postgres", "mssql", "ibm_db2", per_user=True)
def test_sql_node():
    instance_name = ConfigContext.get().instance_name

    script_1_name = {
        "ibm_db2": "script1-db2.sql",
    }.get(instance_name, "script1.sql")
    script_2_name = "script2.sql"

    with Flow("FLOW") as flow:
        with Stage("schema1"):
            parent_dir = Path(__file__).parent
            tab1 = table_1(str(parent_dir / "sql_scripts" / script_1_name))
            tab2 = table_2(str(parent_dir / "sql_scripts" / script_2_name), tab1)
            assert_result(tab2)

    flow_result = flow.run()
    assert flow_result.successful


if __name__ == "__main__":
    test_sql_node()
