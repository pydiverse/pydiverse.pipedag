from __future__ import annotations

from pathlib import Path

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize


@materialize(input_type=sa.Table, lazy=True)
def table_1(script_path: str):
    sql = Path(script_path).read_text()
    return Table(sa.text(sql), name="table_1")


@materialize(input_type=sa.Table, lazy=True)
def table_2(script_path: str, dependent_table: Table):
    sql = Path(script_path).read_text().replace("{{dependent}}", str(dependent_table))
    return Table(sa.text(sql), name="test_table2")


@materialize(input_type=pd.DataFrame, lazy=True)
def assert_result(df: pd.DataFrame):
    pd.testing.assert_frame_equal(df, pd.DataFrame({"coltab2": [24]}))


def test_sql_node():
    with Flow("FLOW") as flow:
        with Stage("schema1"):
            parent_dir = Path(__file__).parent
            tab1 = table_1(str(parent_dir / "sql_scripts" / "script1.sql"))
            tab2 = table_2(str(parent_dir / "sql_scripts" / "script2.sql"), tab1)
            assert_result(tab2)

    flow_result = flow.run()
    assert flow_result.successful


if __name__ == "__main__":
    test_sql_node()
