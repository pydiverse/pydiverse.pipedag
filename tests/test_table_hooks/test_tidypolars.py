from __future__ import annotations

import pytest

from pydiverse.pipedag import *

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, skip_instances, with_instances
from tests.util.tasks_library import assert_table_equal

pytestmark = [
    pytest.mark.polars,
    with_instances(DATABASE_INSTANCES),
    skip_instances("duckdb"),
]


try:
    import tidypolars as tp
except ImportError:
    tp = None


@pytest.mark.skipif(tp is None, reason="Test requires tidypolars to be installed")
def test_table_store():
    @materialize()
    def in_table():
        return Table(
            tp.Tibble(
                {
                    "col": [0, 1, 2, 3],
                }
            )
        )

    @materialize()
    def expected_out_table():
        return Table(
            tp.Tibble(
                {
                    "col": [0, 1, 2, 3],
                    "x": [1, 1, 1, 1],
                    "y": [2, 2, 2, 2],
                }
            )
        )

    @materialize(input_type=tp.Tibble)
    def noop(x):
        return Table(x)

    @materialize(lazy=True, input_type=tp.Tibble)
    def noop_lazy(x):
        return Table(x)

    @materialize(input_type=tp.Tibble)
    def add_column(x):
        return Table(x.mutate(x=1))

    @materialize(lazy=True, input_type=tp.Tibble)
    def add_column_lazy(x):
        return Table(x.mutate(y=2))

    with Flow() as f:
        with Stage("tidypolars"):
            table = in_table()
            table = noop(table)
            table = noop_lazy(table)
            table = add_column(table)
            table = add_column_lazy(table)

            expected = expected_out_table()
            _ = assert_table_equal(table, expected, check_dtype=False)

    assert f.run().successful
