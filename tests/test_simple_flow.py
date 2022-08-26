from __future__ import annotations

import os.path
import time
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Blob, Flow, Stage, Table, materialize


def test_simple_flow():
    @materialize(nout=2, version="1")
    def inputs():
        dfA = pd.DataFrame(
            {
                "a": [0, 1, 2, 4],
                "b": [9, 8, 7, 6],
            }
        )

        dfB = pd.DataFrame(
            {
                "a": [2, 1, 0, 1],
                "x": [1, 1, 2, 2],
            }
        )

        import time

        time.sleep(1)
        return Table(dfA, "dfA"), Table(dfB, "dfA_%%")

    @materialize(input_type=pd.DataFrame)
    def double_values(df: pd.DataFrame):
        return Table(df.transform(lambda x: x * 2))

    @materialize(input_type=sa.Table, lazy=True)
    def join_on_a(left: sa.Table, right: sa.Table):
        return Table(left.select().join(right, left.c.a == right.c.a))

    @materialize(input_type=pd.DataFrame)
    def list_arg(x: list[pd.DataFrame]):
        assert isinstance(x[0], pd.DataFrame)
        return Blob(x)

    @materialize()
    def blob_task(x, y):
        return Blob(x), Blob(y)

    with Flow("FLOW") as flow:
        with Stage("stage1"):
            a, b = inputs()
            a2 = double_values(a)
            b2 = double_values(b)
            b4 = double_values(b2)
            b4 = double_values(b4)
            x = list_arg([a2, b, b4])

        with Stage("stage2"):
            xj = join_on_a(a2, b4)
            a = double_values(xj)

            v = blob_task(x, x)
            v = blob_task(v, v)
            v = blob_task(v, v)

    result = flow.run()
    assert result.successful


if __name__ == "__main__":
    test_simple_flow()


def test_sql_node():
    @materialise(input_type=sa.Table, lazy=True)
    def table_1(script_path: str):
        sql = Path(script_path).read_text()
        return Table(sa.text(sql), name="table_1")

    @materialise(input_type=sa.Table, lazy=True)
    def table_2(script_path: str, dependent_table: Table):
        sql = (
            Path(script_path).read_text().replace("{{dependent}}", str(dependent_table))
        )
        return Table(sa.text(sql), name="test_table2")

    @materialise(input_type=pd.DataFrame, lazy=True)
    def assert_result(df: pd.DataFrame):
        pd.testing.assert_frame_equal(df, pd.DataFrame({"coltab2": [24]}))

    with Flow("FLOW") as flow:
        with Schema("schema1"):
            tab1 = table_1(str(Path(__file__).parent / "script1.sql"))
            tab2 = table_2(str(Path(__file__).parent / "script2.sql"), tab1)
            assert_result(tab2)

    flow_result = flow.run()
    assert flow_result.is_successful()
