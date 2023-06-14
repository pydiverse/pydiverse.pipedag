from __future__ import annotations

import pandas as pd
import pytest
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize, PipedagConfig


@materialize(nout=2, version="1.1")
@pytest.mark.skipif()
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
def join_on_a(left: sa.Table, right: sa.Table):
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


def test_simple_flow():
    flow = get_flow()
    result = flow.run()  # this will use the default configuration instance=__any__
    assert result.successful


def test_alternative_way_to_load_config():
    # this will use the default configuration instance=__any__
    cfg = PipedagConfig.default.get()
    flow = get_flow()
    result = flow.run(cfg)
    assert result.successful


@pytest.mark.mssql
@pytest.mark.parallelize("mssql")
def test_mssql():
    cfg = PipedagConfig.default.get(instance="mssql")
    flow = get_flow()
    result = flow.run(cfg)
    assert result.successful


@pytest.mark.ibm_db2
@pytest.mark.parallelize("ibm_db2")
def test_ibm_db2():
    cfg = PipedagConfig.default.get(instance="ibm_db2")
    flow = get_flow()
    result = flow.run(cfg)
    assert result.successful


if __name__ == "__main__":
    test_simple_flow()
