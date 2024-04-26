from __future__ import annotations

import tempfile

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Schema, Table, materialize
from pydiverse.pipedag.core.config import create_basic_pipedag_config
from pydiverse.pipedag.materialize.debug import materialize_table
from pydiverse.pipedag.util.structlog import setup_logging


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg = get_config(temp_dir)
        engine = cfg.store.table_store.engine

        # We can call tasks interactively which, however, will not do materialization
        # nor dematerialization. Thus we can only link tasks with same input_type:
        a, b = eager_inputs()
        pd.testing.assert_frame_equal(a, dfA)
        a2 = a.rename(columns={"a": "x"})  # we need a dataframe with a column named "x"
        eager = eager_task(a2, b)

        # Same also works for lazy tasks, however, SQLAlchemy expressions are a bit
        # harder to manage manually:
        lazy_1 = lazy_task_1()
        assert (
            str(lazy_1.compile(engine, compile_kwargs={"literal_binds": True}))
            == "SELECT 1 AS x, 2 AS y"
        )

        # We might want to create a custom debugging schema:
        schema = Schema(
            "any_schema_will_do",
            prefix=cfg.store.table_store.schema_prefix,
            suffix=cfg.store.table_store.schema_suffix,
        )
        with engine.connect() as conn:
            conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema.get()}"))
            if sa.__version__ >= "2.0.0":
                conn.commit()
        # We can do this also a bit shorter and with more debugging output:
        cfg.store.table_store.execute(f"CREATE SCHEMA IF NOT EXISTS {schema.get()}")

        # Between lazy tasks we need to materialize.
        # However, we can give a configuration / database URL quite easily:
        tbl = Table(lazy_task_1(), name="task_1_out").materialize(cfg, schema)
        assert tbl.original.name == "task_1_out"
        assert tbl.original.schema == schema.get()

        # It is also possible to explicitly ask for materialize() to return dataframes
        assert (
            Table(lazy_task_1()).materialize(cfg, schema, return_as_type=pd.DataFrame)[
                "x"
            ][0]
            == 1
        )

        # ## now we can call the following flow interactively:
        # with cfg:
        #     with Flow() as f:
        #         with Stage("stage_1"):
        #             lazy_1 = lazy_task_1()
        #             a, b = eager_inputs()
        #
        #         with Stage("stage_2"):
        #             lazy_2 = lazy_task_2(lazy_1, b)
        #             lazy_3 = lazy_task_3(lazy_2)
        #             eager = eager_task(lazy_1, b)
        #
        #         with Stage("stage_3"):
        #             lazy_4 = lazy_task_4(lazy_2)
        #         _ = lazy_3, lazy_4, eager  # unused terminal output tables

        # stage_1:
        stage_1 = Schema("stage_1")
        cfg.store.table_store.execute(f"CREATE SCHEMA IF NOT EXISTS {stage_1.get()}")
        lazy_1 = lazy_task_1()
        a, b = eager_inputs()
        # materializations and dematerializations:
        lazy_1, lazy_1_df = Table(lazy_1).materialize(
            config_context=cfg, schema=stage_1, return_as_type=[None, pd.DataFrame]
        )
        a_df = a
        b_df = b
        b = Table(b).materialize(config_context=cfg, schema=stage_1)
        # stage_2:
        stage_2 = Schema("stage_2")
        cfg.store.table_store.execute(f"CREATE SCHEMA IF NOT EXISTS {stage_2.get()}")
        lazy_2 = lazy_task_2(lazy_1, b)
        lazy_2 = Table(lazy_2, name="task_2_out").materialize(
            config_context=cfg, schema=stage_2
        )
        lazy_3 = lazy_task_3(lazy_2)
        eager = eager_task(lazy_1_df, b_df)
        # stage_3:
        stage_3 = Schema("stage_3")
        cfg.store.table_store.execute(f"CREATE SCHEMA IF NOT EXISTS {stage_3.get()}")
        lazy_4 = lazy_task_4(lazy_2, cfg, stage_3)

        # testing:
        pd.testing.assert_frame_equal(a_df, dfA)
        assert (
            str(lazy_3.compile(engine, compile_kwargs={"literal_binds": True}))
            == 'SELECT * FROM "stage_2"."task_2_out"'
        )
        assert sorted(eager.columns) == ["a", "x", "y"]
        assert (
            str(lazy_4.compile(engine, compile_kwargs={"literal_binds": True}))
            == '\n        SELECT * FROM "stage_2"."task_2_out" as input1\n        '
            'LEFT JOIN "stage_3"."_sub" as sub ON input1.a = sub.a\n    '
        )

        # it is also possible to run code inside tasks in exactly the same way:
        # lazy_4 = lazy_task_4(lazy_2, cfg, stage_3):
        input1 = lazy_2
        subquery = f"""
            SELECT input1.a, sum(input1.x5) as x_sum FROM {ref(input1)} as input1
            GROUP BY a
        """
        sub_ref = Table(sa.text(subquery), name="_sub").materialize(cfg, stage_3)
        query = f"""
            SELECT * FROM {ref(input1)} as input1
            LEFT JOIN {ref(sub_ref)} as sub ON input1.a = sub.a
        """
        lazy_4 = Table(sa.text(query), name="enriched_aggregation").materialize()
        assert (
            str(lazy_4.compile(engine, compile_kwargs={"literal_binds": True}))
            == '\n            SELECT * FROM "stage_2"."task_2_out" as input1\n         '
            '   LEFT JOIN "stage_3"."_sub" as sub ON input1.a = sub.a\n        '
        )

        # eager = eager_task(lazy_1_df, b_df):
        tbl1, tbl2 = lazy_1_df, b_df
        eager = Table(tbl1.merge(tbl2, on="x")).materialize()
        assert sorted(eager.columns) == ["a", "x", "y"]

        # there is a more low level function to debug materialize tables with a few more
        # options exposed:
        materialize_table(
            Table(lazy_task_1(), name="task_1_out"), cfg, stage_1, debug_suffix="_debug"
        )


@materialize(lazy=True)
def lazy_task_1():
    return Table(
        sa.select(
            sa.literal(1).label("x"),
            sa.literal(2).label("y"),
        ),
        name="task_1_out",
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
def lazy_task_4(input1: sa.sql.expression.Alias, config_context=None, schema=None):
    # imperatively materialize a subquery
    subquery = f"""
        SELECT input1.a, sum(input1.x5) as x_sum FROM {ref(input1)} as input1
        GROUP BY a
    """
    sub_ref = Table(sa.text(subquery), name="_sub").materialize(config_context, schema)
    query = f"""
        SELECT * FROM {ref(input1)} as input1
        LEFT JOIN {ref(sub_ref)} as sub ON input1.a = sub.a
    """
    return Table(sa.text(query), name="enriched_aggregation").materialize()


dfA = pd.DataFrame(
    {
        "a": [0, 1, 2, 4],
        "b": [9, 8, 7, 6],
    }
)


@materialize(nout=2, version="1.0.0")
def eager_inputs():
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


def get_config(temp_dir, filename: str = "db"):
    cfg = create_basic_pipedag_config(
        f"duckdb:///{temp_dir}/{filename}.duckdb",
        disable_stage_locking=True,  # This is special for duckdb
        # Attention: If uncommented, stage and task names might be sent to the
        #   following URL. You can self-host kroki if you like:
        #   https://docs.kroki.io/kroki/setup/install/
        # kroki_url="https://kroki.io",
    ).get("default")
    return cfg


if __name__ == "__main__":
    setup_logging()  # you can setup the logging and/or structlog libraries as you wish
    main()
