from __future__ import annotations

import pandas as pd

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.backend.table.sql import adj_pandas_types
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.util.config import PipedagConfig

dfA_source = pd.DataFrame(
    {
        "a": [0, 1, 2, 4],
        "b": [9, 8, 7, 6],
    }
)
dfA = dfA_source.copy()
input_hash = hash(str(dfA))


def has_new_input(dummy_arg):
    """
    Returns whether new input is available via input hash.

    It is a test implementation which can be controlled via package scoped variable like is_changed.

    :param dummy_arg:
        Argument used to test that custom cache invalidation function gets same arguments as task function
    :return:
        hash value of input (stored hash must not exactly be input hash)
    """
    assert dummy_arg == "irrelevant"
    global input_hash
    return input_hash


# noinspection DuplicatedCode
@materialize(nout=2, cache=has_new_input, version="1.0")
def input_task(dummy_arg):
    global dfA
    print(f"dummy_arg={dummy_arg}, dfA=\n{dfA}")
    return Table(dfA, "dfA"), Table(dfA, "dfB")


@materialize(input_type=pd.DataFrame, version="1.0")
def double_values(df: pd.DataFrame):
    return Table(df.transform(lambda x: x * 2))


# noinspection PyTypeChecker
def get_flow():
    with Flow("FLOW") as flow:
        with Stage("stage_1"):
            dummy_arg = "irrelevant"
            a, b = input_task(dummy_arg)
            a2 = double_values(a)

        with Stage("stage_2"):
            b2 = double_values(b)
            a3 = double_values(a2)

    return flow, b2, a3


def test_source_invalidation():
    # trigger reload of input data
    global dfA
    global input_hash

    flow, out1, out2 = get_flow()

    dfA_source_adj = adj_pandas_types(dfA_source)
    with StageLockContext():
        result = flow.run()
        assert result.successful

        v_out1, v_out2 = result.get(out1), result.get(out2)
        pd.testing.assert_frame_equal(dfA_source_adj * 2, v_out1)
        pd.testing.assert_frame_equal(dfA_source_adj * 4, v_out2)

    # modify input without updating input hash => cached version is used
    dfA["a"] = 10 + dfA_source["a"]

    # this run should work from caches and not change outputs
    with StageLockContext():
        result = flow.run()
        assert result.successful

        v_out1, v_out2 = result.get(out1), result.get(out2)
        pd.testing.assert_frame_equal(dfA_source_adj * 2, v_out1)
        pd.testing.assert_frame_equal(dfA_source_adj * 4, v_out2)

    # update input hash trigger reload of new input data
    input_hash = hash(str(dfA))

    with StageLockContext():
        # this run should ignore fresh input at source nodes and not change outputs
        result = flow.run(ignore_fresh_input=True)
        assert result.successful

        v_out1, v_out2 = result.get(out1), result.get(out2)
        pd.testing.assert_frame_equal(dfA_source_adj * 2, v_out1)
        pd.testing.assert_frame_equal(dfA_source_adj * 4, v_out2)

    with StageLockContext():
        result = flow.run()
        assert result.successful

        v_out1, v_out2 = result.get(out1), result.get(out2)
        pd.testing.assert_frame_equal(
            (dfA_source_adj + pd.DataFrame(dict(a=[10, 10, 10, 10], b=0))) * 2,
            v_out1,
        )
        pd.testing.assert_frame_equal(
            (dfA_source_adj + pd.DataFrame(dict(a=[10, 10, 10, 10], b=0))) * 4,
            v_out2,
        )


if __name__ == "__main__":
    test_source_invalidation()
