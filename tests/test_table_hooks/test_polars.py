from __future__ import annotations

import pytest

from pydiverse.pipedag import *

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances
from tests.util.spy import spy_task
from tests.util.tasks_library import assert_table_equal

pytestmark = [
    pytest.mark.polars,
    with_instances(DATABASE_INSTANCES),
]

try:
    import polars as pl
except ImportError:
    pl = None


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


def test_auto_version_1(mocker):
    should_swap = True
    value_to_add = 1

    @materialize(version="1.0", nout=2)
    def in_tables():
        in_table_1 = pl.DataFrame({"col": [1, 2, 3, 4]})
        in_table_2 = pl.DataFrame({"col": [4, 3, 2, 1]})

        return Table(in_table_1), Table(in_table_2)

    @materialize(input_type=pl.LazyFrame, version=AUTO_VERSION, nout=2)
    def swap_tables(tbl1, tbl2):
        if should_swap:
            return Table(tbl2), Table(tbl1)
        return Table(tbl1), Table(tbl2)

    @materialize(input_type=pl.LazyFrame, version=AUTO_VERSION)
    def add_value(tbl: pl.LazyFrame):
        return Table(tbl.with_columns(pl.col("col") + value_to_add))

    with Flow() as f:
        with Stage("lazy_polars"):
            x_in, y_in = in_tables()
            x_swap, y_swap = swap_tables(x_in, y_in)

            x_add = add_value(x_swap)
            y_add = add_value(y_swap)

    f.run()

    in_spy = spy_task(mocker, x_in)
    swap_spy = spy_task(mocker, x_swap)
    x_add_spy = spy_task(mocker, x_add)
    y_add_spy = spy_task(mocker, y_add)

    f.run()

    in_spy.assert_not_called()
    # Called once for AUTO_VERSION
    swap_spy.assert_called_once()
    x_add_spy.assert_called_once()
    y_add_spy.assert_called_once()

    should_swap = False
    f.run()

    in_spy.assert_not_called()
    # Called once for AUTO_VERSION, and once again because auto version changed
    swap_spy.assert_called(2)
    x_add_spy.assert_called(2)
    y_add_spy.assert_called(2)

    value_to_add = 2
    f.run()

    in_spy.assert_not_called()
    swap_spy.assert_called_once()
    x_add_spy.assert_called(2)
    y_add_spy.assert_called(2)


def test_auto_version_2(mocker):
    should_swap_inputs = False

    @materialize(input_type=pl.LazyFrame, version=AUTO_VERSION, nout=2)
    def in_tables():
        in_table_1 = pl.LazyFrame({"col": [1, 2, 3, 4]})
        in_table_2 = pl.LazyFrame({"col": [4, 3, 2, 1]})

        if should_swap_inputs:
            return Table(in_table_2), Table(in_table_1)
        else:
            return Table(in_table_1), Table(in_table_2)

    with Flow() as f:
        with Stage("lazy_polars"):
            in_tables_ = in_tables()

    f.run()
    in_tables_spy = spy_task(mocker, in_tables_)

    f.run()
    in_tables_spy.assert_called_once()

    should_swap_inputs = True
    f.run()
    in_tables_spy.assert_called(2)
