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
    return Table(dfA, "dfA"), Table(dfB, "dfB_%%")


@materialize(input_type=pd.DataFrame)
def double_values(df: pd.DataFrame):
    return Table(df.transform(lambda x: x * 2))


@materialize(input_type=sa.Table, lazy=True)
def join_on_a(left: sa.sql.expression.Alias, right: sa.sql.expression.Alias):
    return Table(left.select().join(right, left.c.a == right.c.a))


@materialize(input_type=pd.DataFrame)
def list_arg(x: list[pd.DataFrame]):
    assert isinstance(x[0], pd.DataFrame)
    return Blob(x)


@materialize
def blob_task(x, y):
    return Blob(x), Blob(y)


def test_simple_flow(with_blob=True):
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

            if with_blob:
                v = blob_task(x, x)
                v = blob_task(v, v)
                v = blob_task(v, v)

                blob_tuple = blob_task(1, 2)

    with StageLockContext():
        result = flow.run()  # this will use the default configuration instance=__any__
        assert result.successful

        # Check result.get works
        res_a = result.get(a, as_type=pd.DataFrame)
        res_b = result.get(b, as_type=pd.DataFrame)
        res_inp = result.get(inp, as_type=pd.DataFrame)
        res_joined = result.get(joined, as_type=pd.DataFrame)
        res_joined_times_2 = result.get(joined_times_2, as_type=pd.DataFrame)

        assert_frame_equal(res_a, dfA, check_dtype=False)
        assert_frame_equal(res_b, dfB, check_dtype=False)
        assert_frame_equal(res_inp[0], dfA, check_dtype=False)
        assert_frame_equal(res_inp[1], dfB, check_dtype=False)
        assert_frame_equal(res_joined * 2, res_joined_times_2)

        result.get(x)
        if with_blob:
            result.get(v)
            assert tuple(result.get(blob_tuple)) == (1, 2)


if __name__ == "__main__":
    test_simple_flow()
