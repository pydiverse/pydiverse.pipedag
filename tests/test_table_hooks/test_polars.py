from __future__ import annotations

import pytest

from pydiverse.pipedag import *

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances
from tests.util.tasks_library import assert_table_equal

pytestmark = [pytest.mark.polars, with_instances(DATABASE_INSTANCES)]

try:
    import polars as pl
except ImportError:
    pl = None


@with_instances("postgres", "mssql", "ibm_db2")
def test_table_store():
    @materialize()
    def in_table():
        return Table(
            pl.DataFrame(
                {
                    "col": [0, 1, 2, 3],
                }
            )
        )

    @materialize()
    def expected_out_table():
        return Table(
            pl.DataFrame(
                {
                    "col": [0, 1, 2, 3],
                    "x": [1, 1, 1, 1],
                    "y": [2, 2, 2, 2],
                }
            )
        )

    @materialize(input_type=pl.DataFrame)
    def noop(x):
        return Table(x)

    @materialize(lazy=True, input_type=pl.DataFrame)
    def noop_lazy(x):
        return Table(x)

    @materialize(input_type=pl.DataFrame)
    def add_column(x: pl.DataFrame):
        return Table(x.with_columns(pl.lit(1).alias("x")))

    @materialize(lazy=True, input_type=pl.DataFrame)
    def add_column_lazy(x: pl.DataFrame):
        return Table(x.with_columns(pl.lit(2).alias("y")))

    with Flow() as f:
        with Stage("polars"):
            table = in_table()
            table = noop(table)
            table = noop_lazy(table)
            table = add_column(table)
            table = add_column_lazy(table)

            expected = expected_out_table()
            _ = assert_table_equal(table, expected, check_dtype=False)

    assert f.run().successful
