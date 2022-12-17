from __future__ import annotations

import pandas as pd
import sqlalchemy as sa
from pandas.testing import assert_frame_equal

from pydiverse.pipedag import Blob, Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext

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


@materialize(nout=2, version="1")
def inputs():
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


@materialize
def blob_task(x, y):
    return Blob(x), Blob(y)


# noinspection PyTypeChecker
def test_simple_flow():
    with Flow() as flow:
        with Stage("simple_flow_stage1"):
            inp = inputs()
            a, b = inp

            a2 = double_values(a)
            b2 = double_values(b)
            b4 = double_values(b2)
            b4 = double_values(b4)
            x = list_arg([a2, b, b4])

        with Stage("simple_flow_stage2"):
            joined = join_on_a(a2, b4)
            joined_times_2 = double_values(joined)

            v = blob_task(x, x)
            v = blob_task(v, v)
            v = blob_task(v, v)

            blob_tuple = blob_task(1, 2)

    with StageLockContext():
        result = flow.run()  # this will use the default configuration instance=__any__
        assert result.successful

        # Check result.get works
        assert_frame_equal(result.get(a, as_type=pd.DataFrame), dfA)
        assert_frame_equal(result.get(b, as_type=pd.DataFrame), dfB)
        assert_frame_equal(result.get(inp, as_type=pd.DataFrame)[0], dfA)
        assert_frame_equal(result.get(inp, as_type=pd.DataFrame)[1], dfB)
        assert_frame_equal(
            result.get(joined, as_type=pd.DataFrame) * 2,
            result.get(joined_times_2, as_type=pd.DataFrame),
        )

        result.get(x)
        result.get(v)
        assert tuple(result.get(blob_tuple)) == (1, 2)


if __name__ == "__main__":
    test_simple_flow()
