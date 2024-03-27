from __future__ import annotations

import pandas as pd

from pydiverse.pipedag import Flow, Stage, Table, materialize
from tests.fixtures.instances import (
    DATABASE_INSTANCES,
    ORCHESTRATION_INSTANCES,
    with_instances,
)


@materialize(version="1.0")
def inputs():
    df_a = pd.DataFrame(
        {
            "a": [0, 1, 2, 4],
            "b": [9, 8, 7, 6],
        }
    )

    return Table(df_a, "dfA")


@materialize(input_type=pd.DataFrame, version="1.0")
def double_a(tables: dict[str, pd.DataFrame]):
    a = tables["dfA"]
    a["a"] = a["a"] * 2
    return Table(a, "dfA2")


@materialize(input_type=pd.DataFrame, version="1.0")
def halfen_table(table: pd.DataFrame):
    return table / 2


def halfen_tables(tables: dict[str, Table]):
    for table_name in tables:
        tables[table_name] = halfen_table(tables[table_name])
    return tables


def get_flow():
    with Flow() as flow:
        with Stage("simple_flow_stage1"):
            a = inputs()
            tables: dict[str, Table] = {"dfA": a}
            a2 = double_a(tables)
            tables["dfA2"] = a2
            halfen_tables(tables)

    return flow


@with_instances(DATABASE_INSTANCES, ORCHESTRATION_INSTANCES)
def test_simple_flow():
    flow = get_flow()
    result = flow.run()
    assert result.successful
