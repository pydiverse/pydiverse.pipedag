from __future__ import annotations

import pandas as pd
import sqlalchemy as sa
from prefect import Flow

from pydiverse.pipedag import Blob, Stage, Table, materialise

# noinspection PyUnresolvedReferences
from .util import setup_pipedag


def test_simple_flow():
    @materialise(nout=2, version="1")
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

    @materialise(input_type=pd.DataFrame)
    def double_values(df: pd.DataFrame):
        return Table(df.transform(lambda x: x * 2))

    @materialise(input_type=sa.Table, lazy=True)
    def join_on_a(left: sa.Table, right: sa.Table):
        return Table(left.select().join(right, left.c.a == right.c.a))

    @materialise(input_type=pd.DataFrame)
    def list_arg(x: list[pd.DataFrame]):
        assert isinstance(x[0], pd.DataFrame)
        return Blob(x)

    @materialise()
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
    assert result.is_successful()
