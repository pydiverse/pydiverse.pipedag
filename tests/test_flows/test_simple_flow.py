from __future__ import annotations

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from tests.fixtures.instances import (
    DATABASE_INSTANCES,
    ORCHESTRATION_INSTANCES,
    with_instances,
)


@materialize(nout=2, version="1.1")
def inputs():
    df_a = pd.DataFrame(
        {
            "a": [0, 1, 2, 4],
            "b": [9, 8, 7, 6],
        }
    )

    df_b = pd.DataFrame(
        {
            "a": [2, 1, 0, 1],
            "x": [1, 1, 2, 2],
        }
    )
    return Table(df_a, "dfA", primary_key=["a"]), Table(df_b, "dfB")


@materialize(input_type=pd.DataFrame, version="1.0")
def double_values(df: pd.DataFrame):
    df["a"] = df["a"] * 2
    return Table(df)


@materialize(input_type=sa.Table, lazy=True)
def join_on_a(left: sa.sql.expression.Alias, right: sa.sql.expression.Alias):
    return Table(left.select().join(right, left.c.a == right.c.a))


# noinspection PyTypeChecker
def get_flow():
    with Flow() as flow:
        with Stage("simple_flow_stage1"):
            a, b = inputs()
            a2 = double_values(a)

        with Stage("simple_flow_stage2"):
            b2 = double_values(b)
            joined = join_on_a(a2, b2)
            _ = joined
    return flow


@with_instances(DATABASE_INSTANCES, ORCHESTRATION_INSTANCES)
def test_simple_flow():
    flow = get_flow()
    result = flow.run()
    assert result.successful


if __name__ == "__main__":
    test_simple_flow()
