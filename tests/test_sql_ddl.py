# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import sqlalchemy as sa

from pydiverse.pipedag import ConfigContext, Flow, Stage, materialize
from pydiverse.pipedag.backend.table.sql.ddl import (
    CreateSchema,
    CreateTableAsSelect,
    DropTable,
    TruncateTable,
    insert_into_in_query,
)
from pydiverse.pipedag.container import Schema
from tests.fixtures.instances import DATABASE_INSTANCES, skip_instances, with_instances
from tests.util.sql import sql_table_expr


def test_insert_into():
    test_pairs = {
        "Select 1": "Select 1 INTO a.b",
        "Select 1 as _from": "Select 1 as _from INTO a.b",
        "Select 1 as afrom": "Select 1 as afrom INTO a.b",
        "Select 1 WHERE TRUE": "Select 1 INTO a.b WHERE TRUE",
        "Select 1 GROUP\nBY x": "Select 1 INTO a.b GROUP\nBY x",
        "Select 1 FROM A GROUP BY x": "Select 1 INTO a.b FROM A GROUP BY x",
        "Select 1 UNION ALL SELECT 2": "Select 1 INTO a.b UNION ALL SELECT 2",
        "Select 1 From X": "Select 1 INTO a.b From X",
        "Select (SELECT 1 FROM Y) From X": "Select (SELECT 1 FROM Y) INTO a.b From X",
        "Select (SELECT (SELECT 1 FROM Z) FROM Y) From X": ("Select (SELECT (SELECT 1 FROM Z) FROM Y) INTO a.b From X"),
        "Select a.[from] from a": "Select a.[from] INTO a.b from a",
        "Select a.[ from ] from a": "Select a.[ from ] INTO a.b from a",
        'Select "from" from a': 'Select "from" INTO a.b from a',
    }
    for raw_query, expected_query in test_pairs.items():
        res = insert_into_in_query(raw_query, "a", "b")
        assert res == expected_query


@with_instances(DATABASE_INSTANCES, "snowflake")
@skip_instances("parquet_backend", "parquet_s3_backend", "parquet_s3_backend_db2")
def test_truncate_table():
    """Test that TruncateTable removes all rows but preserves the table and its schema."""

    @materialize
    def create_populate_truncate_table():
        store = ConfigContext.get().store.table_store
        schema = Schema("truncate_test_schema", prefix="", suffix="")
        table_name = "truncate_test_table"

        store.execute(CreateSchema(schema, if_not_exists=True))
        store.execute(DropTable(table_name, schema, if_exists=True, cascade=True))
        query = sql_table_expr({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        store.execute(CreateTableAsSelect(table_name, schema, query))

        # Verify rows exist before truncate
        preparer = store.engine.dialect.identifier_preparer
        full_name = f"{preparer.format_schema(schema.get())}.{preparer.quote(table_name)}"
        count_query = sa.text(f"SELECT COUNT(*) FROM {full_name}")
        with store.engine_connect() as conn:
            count = conn.execute(count_query).scalar()
        assert count == 3, f"Expected 3 rows before truncate, got {count}"

        # Execute TruncateTable
        store.execute(TruncateTable(table_name, schema))

        # Verify table is empty but still exists
        with store.engine_connect() as conn:
            count = conn.execute(count_query).scalar()
        assert count == 0, f"Expected 0 rows after truncate, got {count}"

    with Flow() as f:
        with Stage("test_stage"):
            create_populate_truncate_table()

    assert f.run().successful
