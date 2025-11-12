# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause
import dataframely as dy
import pandas as pd
import polars as pl
import sqlalchemy as sa

from pydiverse.common.util.structlog import setup_logging
from pydiverse.pipedag import AUTO_VERSION, Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext


@materialize(lazy=True)
def lazy_task_1():
    return Table(
        sa.select(
            sa.literal(1).label("x"),
            sa.literal(2).label("y"),
        ),
        name="lazy_1",
    )


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_2(input1: sa.Alias, input2: sa.Alias):
    query = sa.select(
        (input1.c.x * 5).label("x5"),
        input2.c.a,
    ).select_from(input1.outerjoin(input2, input2.c.x == input1.c.x))

    return Table(query, name="task_2_out", primary_key=["a"])


def ref(tbl: sa.Alias):
    # For case sensitive (mixed capital/lowercase) names, quoting is necessary.
    # But it is recommended to avoid using this with Snowflake since the default is uppercase.
    return f"{tbl.original.schema}.{tbl.original.name}"


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_3(input1: sa.Alias):
    # With ref() it is recommended to use table alias in the query since the actual table name might change
    return sa.text(f"SELECT input1.* FROM {ref(input1)} as input1")


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_4(input1: sa.Alias):
    return sa.text(f"SELECT * FROM {ref(input1)}")


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


@materialize(version=AUTO_VERSION, input_type=pl.LazyFrame)
def eager_task_polars(tbl1: pl.LazyFrame, tbl2: pl.LazyFrame):
    return tbl1.join(tbl2, on="x")


class Tbl1Schema(dy.Schema):
    x = dy.Int32()
    y = dy.Int32()


class OutputSchema(Tbl1Schema):
    a = dy.Int64()


@materialize(version=AUTO_VERSION, input_type=pl.LazyFrame)
def eager_task_dataframely(tbl1: dy.LazyFrame[Tbl1Schema], tbl2: pl.LazyFrame) -> OutputSchema:
    # Pipedag automatically calls Tbl1Schema.cast() and OutputSchema.validate().
    return tbl1.join(tbl2, on="x")


def main():
    with Flow() as f:
        with Stage("stage_1"):
            lazy_1 = lazy_task_1()
            a, b = eager_inputs()

        with Stage("stage_2"):
            lazy_2 = lazy_task_2(lazy_1, b)
            lazy_3 = lazy_task_3(lazy_2)
            # Do the same operation with various syntax alternatives.
            # This is also a demonstration how pipedag interacts with \
            # dataframely.
            out = eager_task(lazy_1, b)
            out2 = eager_task_polars(lazy_1, b)
            out5 = eager_task_dataframely(lazy_1, b)

        with Stage("stage_3"):
            lazy_4 = lazy_task_4(lazy_2)
        _ = lazy_3, lazy_4, out, out2, out5  # unused terminal output tables

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

    # Run this pipeline with (might take a bit longer on first run in pixi environment):
    # ```shell
    # export SNOWFLAKE_ACCOUNT=<use your snowflake instance>;
    # export SNOWFLAKE_PASSWORD=<use secret token>;
    # export SNOWFLAKE_USER=<username>;
    # export SNOWFLAKE_DB_SUFFIX=_adhoc;  # chose unique suffix to avoid collisions
    # pixi run python run_pipeline.py
    # ```

    main()
