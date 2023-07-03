from __future__ import annotations

import uuid

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, materialize
from pydiverse.pipedag.context import ConfigContext
from tests.fixtures.instances import with_instances


@with_instances("postgres", "postgres_unlogged")
def test_postgres_unlogged():
    def uncached(*args, **kwargs):
        return uuid.uuid1().hex

    @materialize(cache=uncached)
    def dataframe():
        return pd.DataFrame({"x": [1]})

    @materialize(cache=uncached)
    def sql_table():
        return sa.select(sa.literal(1).label("x"))

    @materialize(input_type=sa.Table)
    def get_relpersistence(table: sa.Table):
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
            "u" if ConfigContext.get().store.table_store.unlogged_tables else "p"
        )
        assert df["relpersistence"][0] == relpersistence

    with Flow() as f:
        with Stage("stage"):
            df = dataframe()
            tbl = sql_table()
            rp_df = get_relpersistence(df)
            rp_tbl = get_relpersistence(tbl)
            assert_relpersistence(rp_df)
            assert_relpersistence(rp_tbl)

    f.run()
