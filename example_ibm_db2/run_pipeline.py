# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause
import dataframely as dy
import pandas as pd
import polars as pl
import sqlalchemy as sa

import pydiverse.colspec as cs
import pydiverse.transform as pdt
from pydiverse.common.util.structlog import setup_logging
from pydiverse.pipedag import AUTO_VERSION, Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext
from pydiverse.transform.extended import left_join


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


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_3(input1: sa.Alias):
    return sa.text(f"SELECT * FROM {input1.original.schema}.{input1.original.name}")


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_4(input1: sa.Alias):
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


@materialize(version=AUTO_VERSION, input_type=pl.LazyFrame)
def eager_task_polars(tbl1: pl.LazyFrame, tbl2: pl.LazyFrame):
    return tbl1.join(tbl2, on="x")


@materialize(version=AUTO_VERSION, input_type=pdt.Polars)
def eager_task_pdt(tbl1: pdt.Table, tbl2: pdt.Table):
    return tbl1 >> left_join(tbl2, tbl1.x == tbl2.x) >> pdt.alias("eager_task_pdt_out")


# IBM DB2 support of pydiverse.transform is in planning but not completed, yet.
# @materialize(lazy=True, input_type=pdt.SqlAlchemy)
# def lazy_task_pdt(tbl1: pdt.Table, tbl2: pdt.Table):
#     # pydiverse.transform syntax works both on Polars and SQL/duckdb backends.
#     return tbl1 >> left_join(tbl2, tbl1.x == tbl2.x) >> pdt.alias("lazy_task_pdt_out")


class Tbl1Schema(dy.Schema):
    x = dy.Int32()
    y = dy.Int32()


class OutputSchema(Tbl1Schema):
    a = dy.Int64()


@materialize(version=AUTO_VERSION, input_type=pl.LazyFrame)
def eager_task_dataframely(tbl1: dy.LazyFrame[Tbl1Schema], tbl2: pl.LazyFrame) -> OutputSchema:
    # Pipedag automatically calls Tbl1Schema.cast() and OutputSchema.validate().
    return tbl1.join(tbl2, on="x")


@materialize(version=AUTO_VERSION, input_type=pdt.Polars)
def eager_task_dataframely_pdt(tbl1: Tbl1Schema, tbl2: pdt.Table) -> OutputSchema:
    # Pipedag automatically calls Tbl1Schema.cast() and OutputSchema.validate().
    return tbl1 >> left_join(tbl2, tbl1.x == tbl2.x) >> pdt.alias("eager_task_dataframely_pdt_out")


class Tbl1ColSpec(cs.ColSpec):
    x = cs.Int32()
    y = cs.Int32()


class OutputColSpec(Tbl1ColSpec):
    a = cs.Int64()


@materialize(version=AUTO_VERSION, input_type=pl.LazyFrame)
def eager_task_colspec(tbl1: Tbl1ColSpec, tbl2: pl.LazyFrame) -> OutputColSpec:
    # Pipedag automatically calls Tbl1ColSpec.cast_polars() and OutputColSpec.validate_polars()
    # which call dataframely in the background. ColSpec classes can also be used for SQL validations.
    return tbl1.join(tbl2, on="x")


@materialize(version=AUTO_VERSION, input_type=pdt.Polars)
def eager_task_colspec_pdt(tbl1: Tbl1ColSpec, tbl2: pdt.Table) -> OutputColSpec:
    # Pipedag automatically calls Tbl1ColSpec.cast_polars() and OutputColSpec.validate_polars()
    # which call dataframely in the background. ColSpec classes can also be used for SQL validations.
    return tbl1 >> left_join(tbl2, tbl1.x == tbl2.x) >> pdt.alias("eager_task_colspec_pdt_out")


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_colspec(tbl1: Tbl1ColSpec, tbl2: sa.Alias) -> OutputColSpec:
    # IBM DB2 support of pydiverse.transform is in planning but not completed, yet.
    # Thus pipedag does not automatically call OutputColSpec.filter()
    return Table(
        sa.select(tbl1, tbl2.c.a).select_from(tbl1.outerjoin(tbl2, tbl1.c.x == tbl2.c.x)), name="lazy_task_colspec"
    )


# IBM DB2 support of pydiverse.transform is in planning but not completed, yet.
# @materialize(lazy=True, input_type=pdt.SqlAlchemy)
# def lazy_task_colspec_pdt(tbl1: Tbl1ColSpec, tbl2: pdt.Table) -> OutputColSpec:
#     # Pipedag automatically calls OutputColSpec.filter()
#     return tbl1 >> left_join(tbl2, tbl1.x == tbl2.x) >> pdt.alias("lazy_task_colspec_pdt_out")


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
            # dataframely, pydiverse.colspec, and pydiverse.transform.
            out = eager_task(lazy_1, b)
            out2 = eager_task_polars(lazy_1, b)
            out3 = eager_task_pdt(lazy_1, b)
            # out4 = lazy_task_pdt(lazy_1, b)
            out5 = eager_task_dataframely(lazy_1, b)
            out6 = eager_task_dataframely_pdt(lazy_1, b)
            out7 = eager_task_colspec(lazy_1, b)
            out8 = eager_task_colspec_pdt(lazy_1, b)
            out9 = lazy_task_colspec(lazy_1, b)
            # out10 = lazy_task_colspec_pdt(lazy_1, b)

        with Stage("stage_3"):
            lazy_4 = lazy_task_4(lazy_2)
        _ = lazy_3, lazy_4, out, out2, out3, out5, out6, out7, out8, out9  # unused terminal output tables

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

    # Run docker-compose in separate shell to launch IBM DB2 container:
    # ```shell
    # pixi run docker-compose up
    # ```

    # Wait several minutes for container to be ready. You can check the logs with:
    # ```shell
    # docker logs example_ibm_db2-ibm_db2-1 | grep "Setup has completed"
    # ```

    # Run this pipeline with (might take a bit longer on first run in pixi environment):
    # ```shell
    # pixi run python run_pipeline.py
    # ```

    main()
