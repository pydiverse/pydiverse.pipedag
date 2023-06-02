import pytest

from pydiverse.pipedag import materialize, Table, Flow, Stage, PipedagConfig
import sqlalchemy as sa
import pandas as pd

from pydiverse.pipedag.backend.table.sql import sa_select
from pydiverse.pipedag.context import StageLockContext


@pytest.mark.pdtransform
def test_example_flow():
    import pydiverse.transform as pdt
    from pydiverse.transform.lazy import SQLTableImpl
    from pydiverse.transform.core.verbs import left_join, select, mutate, alias

    @materialize(lazy=True)
    def lazy_task_1():
        return sa_select([sa.literal(1).label("x"), sa.literal(2).label("y")])

    @materialize(lazy=True, input_type=SQLTableImpl)
    def lazy_task_2(input1: pdt.Table, input2: pdt.Table):
        out = (
            input1
            >> left_join(input2 >> select(), input1.x == input2.x)
            >> mutate(x5=input1.x * 5, a=input2.a)
        )
        return Table(out, name="task_2_out", primary_key=["a"])

    @materialize(lazy=True, input_type=SQLTableImpl)
    def lazy_task_3(input: pdt.Table):
        return input >> mutate(xx=input.x * input.x) >> alias("task_3_out")

    @materialize(lazy=True, input_type=SQLTableImpl)
    def lazy_task_4(input: pdt.Table):
        return input

    @materialize(nout=2, version="1.0.0")
    def eager_inputs():
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
        return Table(dfA, "dfA"), Table(dfB, "dfB_%%")

    @materialize(version="1.0.0", input_type=pd.DataFrame)
    def eager_task(tbl1: pd.DataFrame, tbl2: pd.DataFrame):
        return tbl1.merge(tbl2, on="x")

    def main():
        pipedag_config = PipedagConfig.default
        cfg = pipedag_config.get(instance="pdtransform")

        with Flow() as f:
            with Stage("stage_1"):
                lazy_1 = lazy_task_1()
                a, b = eager_inputs()

            with Stage("stage_2") as stage2:
                lazy_2 = lazy_task_2(lazy_1, b)
                lazy_3 = lazy_task_3(lazy_2)
                eager = eager_task(lazy_1, b)

            with Stage("stage_3"):
                lazy_4 = lazy_task_4(lazy_2)
            _ = lazy_3, lazy_4, eager  # unused terminal output tables

        # Run flow
        result = f.run(cfg)
        assert result.successful

        # Run in a different way for testing
        with StageLockContext():
            result = f.run(cfg)
            assert result.successful
            assert result.get(lazy_1, as_type=pd.DataFrame)["x"][0] == 1

    main()
