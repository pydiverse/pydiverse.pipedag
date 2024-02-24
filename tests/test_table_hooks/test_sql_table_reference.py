from __future__ import annotations

import pandas as pd

from pydiverse.pipedag import *
from pydiverse.pipedag.backend.table.sql import TableReference
from pydiverse.pipedag.backend.table.sql.ddl import (
    CreateSchema,
    CreateTableAsSelect,
    DropSchema,
    Schema,
)
from pydiverse.pipedag.context import TaskContext

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances
from tests.util import swallowing_raises
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
        table_store.execute(
            CreateTableAsSelect(
                "table_reference",
                schema,
                query,
            )
        )

        # Return a table reference
        return Table(TableReference(), "table_reference")

    @materialize(version="1.0")
    def in_table_external_schema():
        table_store = ConfigContext.get().store.table_store
        schema = Schema("user_controlled_schema", prefix="", suffix="")
        table_store.execute(DropSchema(schema, if_exists=True, cascade=True))
        table_store.execute(CreateSchema(schema))
        query = sql_table_expr({"col": [4, 5, 6, 7]})
        table_store.execute(
            CreateTableAsSelect(
                "external_table",
                schema,
                query,
            )
        )
        return Table(TableReference(external_schema=schema.get()), "external_table")

    @materialize()
    def expected_out_table():
        return Table(
            pd.DataFrame(
                {
                    "col": [0, 1, 2, 3],
                }
            )
        )

    @materialize()
    def expected_out_table_external_schema():
        return Table(
            pd.DataFrame(
                {
                    "col": [4, 5, 6, 7],
                }
            )
        )

    with Flow() as f:
        with Stage("sql_table_reference"):
            table = in_table()
            expected = expected_out_table()
            _ = assert_table_equal(table, expected, check_dtype=False)
        with Stage("sql_table_reference_external_schema"):
            external_table = in_table_external_schema()
            expected_external_table = expected_out_table_external_schema()
            _ = assert_table_equal(
                external_table, expected_external_table, check_dtype=False
            )

    assert f.run().successful
    assert f.run().successful


def test_bad_table_reference():
    @materialize()
    def bad_table_reference():
        return Table(TableReference(), "this_table_does_not_exist")

    with Flow() as f:
        with Stage("sql_table_reference"):
            bad_table_reference()

    with swallowing_raises(ValueError, match="this_table_does_not_exist"):
        f.run()
