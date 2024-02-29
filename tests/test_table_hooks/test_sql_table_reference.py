from __future__ import annotations

import pandas as pd
import pytest
import sqlalchemy as sa
from sqlalchemy.exc import ProgrammingError

import tests.util.tasks_library as m
from pydiverse.pipedag import *
from pydiverse.pipedag.backend.table.sql import ExternalTableReference
from pydiverse.pipedag.backend.table.sql.ddl import (
    CreateSchema,
    CreateTableAsSelect,
    CreateViewAsSelect,
    DropTable,
    DropView,
    Schema,
)

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances
from tests.util import swallowing_raises
from tests.util.sql import sql_table_expr

pytestmark = [with_instances(DATABASE_INSTANCES)]


@pytest.mark.polars
def test_table_store():
    @materialize(version="1.0")
    def in_table():
        table_store = ConfigContext.get().store.table_store
        schema = Schema("user_controlled_schema", prefix="", suffix="")
        table_name = "external_table"
        table_store.execute(CreateSchema(schema, if_not_exists=True))
        try:
            table_store.execute(DropView("external_view", schema))
        except ProgrammingError:
            pass
        table_store.execute(DropTable(table_name, schema, if_exists=True))
        query = sql_table_expr({"col": [0, 1, 2, 3]})
        table_store.execute(
            CreateTableAsSelect(
                table_name,
                schema,
                query,
            )
        )
        return Table(ExternalTableReference(table_name, schema=schema.get()))

    @materialize(version="1.0", input_type=sa.Table)
    def in_view(tbl: sa.Table):
        table_store = ConfigContext.get().store.table_store
        schema = Schema("user_controlled_schema", prefix="", suffix="")
        view_name = "external_view"
        try:
            # We cannot use if_exists=True here because DB2 does not support it
            table_store.execute(DropView(view_name, schema))
        except ProgrammingError:
            pass
        query = sa.select(tbl.c.col).where(tbl.c.col > 1)
        table_store.execute(
            CreateViewAsSelect(
                view_name,
                schema,
                query,
            )
        )
        return Table(ExternalTableReference(view_name, schema=schema.get()))

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
    def expected_out_view():
        return Table(
            pd.DataFrame(
                {
                    "col": [2, 3],
                }
            )
        )

    with Flow() as f:
        with Stage("sql_table_reference"):
            external_table = in_table()
            expected_external_table = expected_out_table()
            _ = m.assert_table_equal(
                external_table, expected_external_table, check_dtype=False
            )

        with Stage("sql_view_reference"):
            external_view = in_view(external_table)
            expected_external_view = expected_out_view()
            _ = m.assert_table_equal(
                external_view, expected_external_view, check_dtype=False
            )
            external_view_polars = m.noop_polars(external_view)
            external_view_lazy_polars = m.noop_lazy_polars(external_view)
            _ = m.assert_table_equal(
                external_view_polars, expected_external_view, check_dtype=False
            )
            _ = m.assert_table_equal(
                external_view_lazy_polars, expected_external_view, check_dtype=False
            )

    assert f.run().successful
    assert f.run().successful


def test_bad_table_reference():
    @materialize()
    def bad_table_reference():
        return Table(
            ExternalTableReference(
                name="this_table_does_not_exist", schema="ext_schema"
            ),
        )

    with Flow() as f:
        with Stage("sql_table_reference"):
            bad_table_reference()

    with swallowing_raises(ValueError, match="this_table_does_not_exist"):
        f.run()
