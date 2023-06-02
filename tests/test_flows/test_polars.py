import pytest

from pydiverse.pipedag import materialize, Table, Flow, Stage, PipedagConfig
import pandas as pd

from pydiverse.pipedag.context import StageLockContext


@pytest.mark.polars
def test_example_flow():
    import polars
    import tidypolars
    from tidypolars import col

    @materialize(version="1.0.0")
    def tidy_task_1():
        return polars.dataframe.DataFrame(dict(x=[1], y=[2]))

    @materialize(version="1.0.0", input_type=tidypolars.Tibble)
    def tidy_task_2(input1: tidypolars.Tibble, input2: tidypolars.Tibble):
        query = (
            input1.left_join(input2, on="x")
            .mutate(x5=col("x") * 5, a=col("a"))
            .select(["x5", "a"])
        )
        return Table(query, name="task_2_out", primary_key=["a"])

    @materialize(version="1.0.0", input_type=tidypolars.Tibble)
    def tidy_task_3(input: tidypolars.Tibble):
        return input.mutate(aa=col("a") * 2)

    @materialize(version="1.0.0", input_type=tidypolars.Tibble)
    def tidy_task_4(input: tidypolars.Tibble):
        return input

    @materialize(nout=2, version="1.0.0")
    def polars_inputs():
        dfA = polars.dataframe.DataFrame(
            {
                "a": [0, 1, 2, 4],
                "b": [9, 8, 7, 6],
            }
        )
        dfB = polars.dataframe.DataFrame(
            {
                "a": [2, 1, 0, 1],
                "x": [1, 1, 2, 2],
            }
        )
        return Table(dfA, "dfA"), Table(dfB, "dfB_%%")

    @materialize(version="1.0.0", input_type=polars.dataframe.DataFrame)
    def polars_task(tbl1: polars.dataframe.DataFrame, tbl2: polars.dataframe.DataFrame):
        return tbl1.join(tbl2, on="x", how="left")

    def main():
        pipedag_config = PipedagConfig.default
        cfg = pipedag_config.get(instance="polars")

        with Flow() as f:
            with Stage("stage_1"):
                lazy_1 = tidy_task_1()
                a, b = polars_inputs()

            with Stage("stage_2") as stage2:
                lazy_2 = tidy_task_2(lazy_1, b)
                lazy_3 = tidy_task_3(lazy_2)
                eager = polars_task(lazy_1, b)

            with Stage("stage_3"):
                lazy_4 = tidy_task_4(lazy_2)
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
