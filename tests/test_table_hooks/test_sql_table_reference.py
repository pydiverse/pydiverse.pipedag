from __future__ import annotations

import pandas as pd

from pydiverse.pipedag import *
from pydiverse.pipedag.backend.table.sql import TableReference
from pydiverse.pipedag.backend.table.sql.ddl import (
    CreateSchema,
    CreateTableAsSelect,
    DropTable,
    Schema,
)

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances
from tests.util import swallowing_raises
from tests.util.sql import sql_table_expr
from tests.util.tasks_library import assert_table_equal

pytestmark = [with_instances(DATABASE_INSTANCES)]


def test_table_store():
    @materialize(version="1.0")
    def in_table():
        table_store = ConfigContext.get().store.table_store
        schema = Schema("user_controlled_schema", prefix="", suffix="")
        table_name = "external_table"
        table_store.execute(CreateSchema(schema, if_not_exists=True))
        table_store.execute(DropTable(table_name, schema, if_exists=True))
        query = sql_table_expr({"col": [0, 1, 2, 3]})
        table_store.execute(
            CreateTableAsSelect(
                table_name,
                schema,
                query,
            )
        )
        return Table(TableReference(external_schema=schema.get()), table_name)

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
            external_table = in_table()
            expected_external_table = expected_out_table()
            _ = assert_table_equal(
                external_table, expected_external_table, check_dtype=False
            )

    assert f.run().successful
    assert f.run().successful


def test_bad_table_reference():
    @materialize()
    def bad_table_reference():
        return Table(
            TableReference(external_schema="ext_schema"), "this_table_does_not_exist"
        )

    with Flow() as f:
        with Stage("sql_table_reference"):
            bad_table_reference()

    with swallowing_raises(ValueError, match="this_table_does_not_exist"):
        f.run()
