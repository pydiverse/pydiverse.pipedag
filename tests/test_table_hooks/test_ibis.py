from __future__ import annotations

import pandas as pd
import pytest

from pydiverse.pipedag import *

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, skip_instances, with_instances
from tests.util.tasks_library import assert_table_equal

pytestmark = [pytest.mark.ibis, with_instances(DATABASE_INSTANCES)]


try:
    import ibis
except ImportError:
    ibis = None


# connectorx and thus ibis have trouble with db2+ibm_db:// URLs and mssql
@skip_instances("ibm_db2", "mssql")
def test_table_store():
    IbisTable = ibis.api.Table

    @materialize()
    def in_table():
        return Table(
            pd.DataFrame(
                {
                    "col": [0, 1, 2, 3],
                }
            )
        )

    @materialize()
    def expected_out_table():
        return Table(
            pd.DataFrame(
                {
                    "col": [0, 1, 2, 3],
                    "x": [1, 1, 1, 1],
                    "y": [2, 2, 2, 2],
                }
            )
        )

    @materialize(input_type=IbisTable)
    def noop(x):
        return Table(x)

    @materialize(lazy=True, input_type=IbisTable)
    def noop_lazy(x):
        return Table(x)

    @materialize(input_type=IbisTable)
    def add_column(x: IbisTable):
        return Table(x.mutate(x=ibis.literal(1)))

    @materialize(lazy=True, input_type=IbisTable)
    def add_column_lazy(x: IbisTable):
        return Table(x.mutate(y=ibis.literal(2)))

    with Flow() as f:
        with Stage("ibis"):
            table = in_table()
            table = noop(table)
            table = noop_lazy(table)
            table = add_column(table)
            table = add_column_lazy(table)

            expected = expected_out_table()
            _ = assert_table_equal(table, expected, check_dtype=False)

    assert f.run().successful
