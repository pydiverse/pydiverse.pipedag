from __future__ import annotations

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.util.structlog import setup_logging


@materialize(lazy=True)
def lazy_task_1():
    return sa.select(
        sa.literal(1).label("x"),
        sa.literal(2).label("y"),
    )


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_2(input1: sa.sql.expression.Alias, input2: sa.sql.expression.Alias):
    query = sa.select(
        (input1.c.x * 5).label("x5"),
        input2.c.a,
    ).select_from(input1.outerjoin(input2, input2.c.x == input1.c.x))

    return Table(query, name="task_2_out", primary_key=["a"])


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_3(input1: sa.sql.expression.Alias):
    return sa.text(f"SELECT * FROM {input1.original.schema}.{input1.original.name}")


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_4(input1: sa.sql.expression.Alias):
    return sa.text(f"SELECT * FROM {input1.original.schema}.{input1.original.name}")


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
    with Flow() as f:
        with Stage("stage_1"):
            lazy_1 = lazy_task_1()
            a, b = eager_inputs()

        with Stage("stage_2"):
            lazy_2 = lazy_task_2(lazy_1, b)
            lazy_3 = lazy_task_3(lazy_2)
            eager = eager_task(lazy_1, b)

        with Stage("stage_3"):
            lazy_4 = lazy_task_4(lazy_2)
        _ = lazy_3, lazy_4, eager  # unused terminal output tables

    # Run flow
    result = f.run()
    assert result.successful

    # Run in a different way for testing
    with StageLockContext():
        result = f.run()
        assert result.successful
        assert result.get(lazy_1, as_type=pd.DataFrame)["x"][0] == 1


if __name__ == "__main__":
    setup_logging()  # you can setup the logging and/or structlog libraries as you wish
    main()
