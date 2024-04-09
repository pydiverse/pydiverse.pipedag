from __future__ import annotations

import tempfile

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.core.config import create_basic_pipedag_config
from pydiverse.pipedag.util.structlog import setup_logging


@materialize(lazy=True)
def lazy_task_1():
    return Table(
        sa.select(
            sa.literal(1).label("x"),
            sa.literal(2).label("y"),
        )
    ).materialize()


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_2(input1: sa.sql.expression.Alias, input2: sa.sql.expression.Alias):
    query = sa.select(
        (input1.c.x * 5).label("x5"),
        input2.c.a,
    ).select_from(input1.outerjoin(input2, input2.c.x == input1.c.x))

    return Table(query, name="task_2_out", primary_key=["a"]).materialize()


def ref(tbl: sa.sql.expression.Alias):
    return f'"{tbl.original.schema}"."{tbl.original.name}"'


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_3(input1: sa.sql.expression.Alias):
    return Table(sa.text(f"SELECT * FROM {ref(input1)}")).materialize()


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_4(input1: sa.sql.expression.Alias):
    # imperatively materialize a subquery
    subquery = f"""
        SELECT input1.a, sum(input1.x5) as x_sum FROM {ref(input1)} as input1
        GROUP BY a
    """
    sub_ref = Table(sa.text(subquery), name="_sub").materialize()
    query = f"""
        SELECT * FROM {ref(input1)} as input1
        LEFT JOIN {ref(sub_ref)} as sub ON input1.a = sub.a
    """
    return Table(sa.text(query), name="enriched_aggregation").materialize()


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
    return Table(dfA, "dfA").materialize(), Table(dfB, "dfB_%%").materialize()


@materialize(version="1.0.0", input_type=pd.DataFrame)
def eager_task(tbl1: pd.DataFrame, tbl2: pd.DataFrame):
    return Table(tbl1.merge(tbl2, on="x")).materialize()


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg = create_basic_pipedag_config(
            f"duckdb:///{temp_dir}/db.duckdb",
            disable_stage_locking=True,  # This is special for duckdb
            # Attention: If uncommented, stage and task names might be sent to the
            #   following URL. You can self-host kroki if you like:
            #   https://docs.kroki.io/kroki/setup/install/
            # kroki_url="https://kroki.io",
        ).get("default")
        with cfg:
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
