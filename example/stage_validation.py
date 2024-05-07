from __future__ import annotations

import logging
import tempfile

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, input_stage_versions, materialize
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.core.config import create_basic_pipedag_config
from pydiverse.pipedag.util.structlog import setup_logging


# The @input_stage_versions decorator offers all the options that @materialize has as
# well. `input_type=sa.Table` may be a good choice for tests on larger tables.
@input_stage_versions(input_type=pd.DataFrame)
def validate_stage1(tbls: dict[str, pd.DataFrame], other_tbls: dict[str, pd.DataFrame]):
    # Any tests can be done on tables of both versions. They can either fail and prevent
    # the schema swap, print some information about differences, or produce a table
    # with results of validation.
    assert tbls["task_1_out"]["x"][0] == 1
    assert tbls["task_2_out"]["a"].sum() == 3
    tables = {t.lower() for t in tbls.keys() if not t.endswith("__copy")}
    assert len(tables - {"task_1_out", "task_2_out", "dfa"}) == 1
    assert list(tables - {"task_1_out", "task_2_out", "dfa"})[0].startswith("dfb_")
    logger = logging.getLogger(f"{__name__}-validate_stage1")
    logger.info("Additional tables: %s", set(tbls.keys()) - set(other_tbls.keys()))
    logger.info(
        "Missing tables: %s",
        set(other_tbls.keys()) - set(tbls.keys()) - {"column_diffs"},
    )

    # Producing a table with differences of matching table names can be done a lot more
    # elaborate in a library. Here is just an idea to get started:
    def get_missing_columns(tbl: pd.DataFrame, other_tbl: pd.DataFrame):
        return set(other_tbl.columns) - set(tbl.columns)

    missing_columns = {
        tbl: get_missing_columns(tbls[tbl], other_tbls[tbl])
        for tbl in set(tbls.keys()) & set(other_tbls.keys())
    }
    col_diff_dfs = [pd.DataFrame(dict(table=[], column=[], value=[]))]
    for tbl, columns in missing_columns.items():
        col_diff_df = pd.DataFrame(
            dict(
                table=tbl,
                column=list(columns),
                value="missing",
            )
        )
        col_diff_dfs.append(col_diff_df)
    col_diff_df = pd.concat(col_diff_dfs, ignore_index=True, axis="rows")
    return Table(col_diff_df, name="column_diffs")


@materialize(lazy=True)
def lazy_task_1():
    return Table(
        sa.select(
            sa.literal(1).label("x"),
            sa.literal(2).label("y"),
        ),
        name="task_1_out",
    )


@materialize(lazy=True, input_type=sa.Table)
def lazy_task_2(input1: sa.sql.expression.Alias, input2: sa.sql.expression.Alias):
    query = sa.select(
        (input1.c.x * 5).label("x5"),
        input2.c.a,
    ).select_from(input1.outerjoin(input2, input2.c.x == input1.c.x))

    return Table(query, name="task_2_out")


add_column = {"c": [1, 2, 3, 4]}


@materialize(nout=2)
def eager_inputs():
    dfA = pd.DataFrame(
        {
            "a": [0, 1, 2, 4],
            "b": [9, 8, 7, 6],
            **add_column,
        }
    )
    dfB = pd.DataFrame(
        {
            "a": [2, 1, 0, 1],
            "x": [1, 1, 2, 2],
        }
    )
    return Table(dfA, "dfA"), Table(dfB, "dfB_%%")


def main():
    logger = logging.getLogger(__name__)
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
                    lazy_2 = lazy_task_2(lazy_1, b)
                    col_diff = validate_stage1()

                _ = a, lazy_2  # unused terminal output tables

            # Run Flow a bit different first
            result = f.run()
            assert result.successful

            # change something in table dfa
            global add_column
            add_column = {}

            # Run Flow and print diff result
            with StageLockContext():
                result = f.run()
                assert result.successful
                logger.info(
                    "Column differences:\n%s",
                    result.get(col_diff, as_type=pd.DataFrame),
                )


if __name__ == "__main__":
    setup_logging()  # you can setup the logging and/or structlog libraries as you wish
    main()
