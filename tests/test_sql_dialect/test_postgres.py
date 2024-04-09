from __future__ import annotations

import uuid

import pandas as pd
import sqlalchemy as sa
import structlog

from pydiverse.pipedag import Flow, Stage, materialize
from pydiverse.pipedag.context import ConfigContext
from tests.fixtures.instances import with_instances


@with_instances("postgres", "postgres_unlogged")
def test_postgres_unlogged():
    @materialize(version="1.0.0")
    def dataframe(manual_invalidate):
        _ = manual_invalidate
        return pd.DataFrame({"x": [1]})

    @materialize(lazy=True)
    def sql_table(manual_invalidate):
        _ = manual_invalidate
        return sa.select(sa.literal(1).label("x"))

    @materialize(input_type=sa.Table)
    def get_relpersistence(table: sa.sql.expression.Alias):
        return sa.text(
            """
            SELECT relpersistence
            FROM pg_class
            LEFT JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
            WHERE nspname = :schema
              AND relname = :name
            """
        ).bindparams(
            schema=str(table.original.schema),
            name=str(table.original.name),
        )

    @materialize(input_type=pd.DataFrame)
    def assert_relpersistence(df: pd.DataFrame):
        relpersistence = (
            "u"
            if ConfigContext.get()
            .store.table_store.materialization_details["__any__"]
            .unlogged
            else "p"
        )
        assert df["relpersistence"][0] == relpersistence

    def get_flow(manual_invalidate, partial_invalidate):
        with Flow() as f:
            with Stage("stage"):
                df = dataframe(manual_invalidate)
                tbl = sql_table(manual_invalidate)
                # just to prevent 100% cache validity
                _ = sql_table(partial_invalidate)
            with Stage("check"):
                rp_df = get_relpersistence(df)
                rp_tbl = get_relpersistence(tbl)
                assert_relpersistence(rp_df)
                assert_relpersistence(rp_tbl)
        return f

    manual_invalidate = str(uuid.uuid4())
    partial_invalidate = str(uuid.uuid4())

    logger = structlog.get_logger("test_postgres_unlogged")
    logger.info("1st run")
    f = get_flow(manual_invalidate, partial_invalidate)
    f.run()

    logger.info("2nd run with 100% cache valid stage")
    f.run()

    logger.info("3rd run with partial cache invalid stage")
    partial_invalidate = str(uuid.uuid4())
    f = get_flow(manual_invalidate, partial_invalidate)
    f.run()
