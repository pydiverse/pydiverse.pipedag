from __future__ import annotations

import pandas as pd
import pytest
import sqlalchemy as sa

from pydiverse.pipedag import Blob, Flow, Stage, Table, materialize
from pydiverse.pipedag.util.config import PipedagConfig


@materialize(nout=2, version="1.0")
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
    return Table(dfA, "dfA"), Table(dfB, "dfB")


@materialize(input_type=pd.DataFrame, version="1.0")
def double_values(df: pd.DataFrame):
    df["a"] = df["a"] * 2
    return Table(df)


# noinspection DuplicatedCode
@materialize(input_type=sa.Table, lazy=True)
def join_on_a(left: sa.Table, right: sa.Table):
    return Table(left.select().join(right, left.c.a == right.c.a))


# noinspection PyTypeChecker
def _get_flow(name="default"):
    with Flow(name) as flow:
        with Stage("simple_flow_stage1"):
            a, b = inputs()

            a2 = double_values(a)

        with Stage("simple_flow_stage2"):
            b2 = double_values(b)
            joined = join_on_a(a2, b2)
            _ = joined
    return flow


def test_simple_flow():
    flow = _get_flow()
    result = flow.run()  # this will use the default configuration instance=__any__
    assert result.successful


def test_alternative_way_to_load_config():
    # this will use the default configuration instance=__any__
    cfg = PipedagConfig.default.get()
    flow = _get_flow(cfg.pipedag_name)
    result = flow.run(cfg)
    assert result.successful


@pytest.mark.mssql
def test_mssql():
    # this will use the default configuration instance=__any__
    cfg = PipedagConfig.default.get(instance="mssql")
    flow = _get_flow(cfg.pipedag_name)
    result = flow.run(cfg)
    assert result.successful


@pytest.mark.ibm_db2
def test_ibm_db2():
    # this will use the default configuration instance=__any__
    cfg = PipedagConfig.default.get(instance="ibm_db2")
    flow = _get_flow(cfg.pipedag_name)
    result = flow.run(cfg)
    assert result.successful


if __name__ == "__main__":
    test_simple_flow()
