from __future__ import annotations

import pandas as pd
import pytest

from pydiverse.pipedag import *
from pydiverse.pipedag.backend.table.sql import TableReference
from pydiverse.pipedag.backend.table.sql.ddl import CreateTableAsSelect
from pydiverse.pipedag.context import TaskContext

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances
from tests.util.sql import sql_table_expr
from tests.util.tasks_library import assert_table_equal

pytestmark = [with_instances(DATABASE_INSTANCES)]


def test_table_store():
    @materialize(version="1.0")
    def in_table():
        task = TaskContext.get().task
        table_store = ConfigContext.get().store.table_store
        schema = table_store.get_schema(task.stage.transaction_name)

        # Manually materialize the table
        query = sql_table_expr({"col": [0, 1, 2, 3]})
        with table_store.engine.begin() as conn:
            conn.execute(
                CreateTableAsSelect(
                    "table_reference",
                    schema,
                    query,
                )
            )

        # Return a table reference
        return Table(TableReference(), "table_reference")

    @materialize()
    def expected_out_table():
        return Table(
            pd.DataFrame(
                {
                    "col": [0, 1, 2, 3],
                }
            )
        )

    with Flow() as f:
        with Stage("sql_table_reference"):
            table = in_table()
            expected = expected_out_table()
            _ = assert_table_equal(table, expected, check_dtype=False)

    assert f.run().successful
    assert f.run().successful


def test_bad_table_reference():
    @materialize()
    def bad_table_reference():
        return Table(TableReference(), "this_table_does_not_exist")

    with Flow() as f:
        with Stage("sql_table_reference"):
            bad_table_reference()

    with ConfigContext.get().evolve(swallow_exceptions=True):
        with pytest.raises(ValueError, match="this_table_does_not_exist"):
            f.run()
