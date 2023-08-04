from __future__ import annotations

import pandas as pd

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext
from tests.util.spy import spy_task

dfA_source = pd.DataFrame(
    {
        "a": [0, 1, 2, 4],
        "b": [9, 8, 7, 6],
    }
)
dfA = dfA_source.copy()
input_hash = str(dfA)


def has_new_input(dummy_arg):
    """Returns whether new input is available via input hash.

    :param dummy_arg: Argument used to test that custom cache invalidation function
        gets same arguments as task function
    :return: hash value of input (stored hash must not exactly be input hash)
    """
    assert dummy_arg == "irrelevant"
    global input_hash
    return input_hash


# noinspection DuplicatedCode
@materialize(nout=2, cache=has_new_input, version="1.0")
def input_task(dummy_arg):
    global dfA
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


def test_source_invalidation(mocker):
    # trigger reload of input data
    global dfA
    global input_hash

    flow, out1, out2 = get_flow()

    out1_spy = spy_task(mocker, out1)
    out2_spy = spy_task(mocker, out2)

    with StageLockContext():
        result = flow.run()
        assert result.successful

        v_out1, v_out2 = result.get(out1), result.get(out2)
        pd.testing.assert_frame_equal(dfA_source * 2, v_out1, check_dtype=False)
        pd.testing.assert_frame_equal(dfA_source * 4, v_out2, check_dtype=False)

        out1_spy.assert_called_at_most(1)
        out2_spy.assert_called_at_most(1)

    # modify input without updating input hash => cached version is used
    dfA["a"] = 10 + dfA_source["a"]

    # this run should work from caches and not change outputs
    with StageLockContext():
        result = flow.run()
        assert result.successful

        v_out1, v_out2 = result.get(out1), result.get(out2)
        pd.testing.assert_frame_equal(dfA_source * 2, v_out1, check_dtype=False)
        pd.testing.assert_frame_equal(dfA_source * 4, v_out2, check_dtype=False)

        out1_spy.assert_not_called()
        out2_spy.assert_not_called()

    # update input hash trigger reload of new input data
    input_hash = str(dfA)

    with StageLockContext():
        # this run should not result in any task being called,
        # because ignore_cache_function is set to True
        result = flow.run(ignore_cache_function=True)
        assert result.successful

        out1_spy.assert_not_called()
        out2_spy.assert_not_called()

    with StageLockContext():
        result = flow.run()
        assert result.successful

        v_out1, v_out2 = result.get(out1), result.get(out2)
        pd.testing.assert_frame_equal(dfA * 2, v_out1, check_dtype=False)
        pd.testing.assert_frame_equal(dfA * 4, v_out2, check_dtype=False)

        out1_spy.assert_called_at_most(1)
        out2_spy.assert_called_at_most(1)


if __name__ == "__main__":
    test_source_invalidation()
