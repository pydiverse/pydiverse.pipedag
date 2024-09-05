from __future__ import annotations

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import *
from pydiverse.pipedag.backend.table.sql.ddl import (
    CreateSchema,
    CreateTableAsSelect,
    DropTable,
    InsertIntoSelect,
)
from pydiverse.pipedag.container import ExternalTableReference, Schema
from tests.fixtures.instances import with_instances

# Parameterize all tests in this file with several instance_id configurations
from tests.util.sql import sql_table_expr


def test_external_table_inputs():
    @materialize(version="1.1")
    def make_external_table():
        table_store = ConfigContext.get().store.table_store
        schema = Schema("external_schema", prefix="", suffix="")
        table_name = "external_table"
        table_store.execute(CreateSchema(schema, if_not_exists=True))
        table_store.execute(DropTable(table_name, schema, if_exists=True))
        query = sql_table_expr({"col": [0, 1, 2, 3]})
        cmds = (
            [CreateTableAsSelect, InsertIntoSelect]
            if table_store.engine.dialect.name == "ibm_db_sa"
            else [CreateTableAsSelect]
        )
        for cmd in cmds:
            table_store.execute(
                cmd(
                    table_name,
                    schema,
                    query,
                )
            )
        return Table(ExternalTableReference(table_name, schema=schema.get()))

    @materialize(input_type=sa.Table)
    def duplicate_table_reference():
        return Table(pd.DataFrame())

    @materialize(input_type=pd.DataFrame)
    def identity(table: pd.DataFrame):
        return table

    with Flow() as f:
        with Stage("sql_table_origin"):
            _ = make_external_table()

        with Stage("sql_table_linked"):
            table = duplicate_table_reference()
            output = identity(table)

    with StageLockContext():
        # Normal execution. duplicate_table_reference should return an empty table
        result = f.run()
        assert result.get(output, as_type=pd.DataFrame).shape[0] == 0

    with StageLockContext():
        # Linked execution. Body of duplicate_table_reference should not be executed,
        # instead output is referenced from the linked table
        result = f.run(
            inputs={
                table: ExternalTableReference(
                    "external_table",
                    schema="external_schema",
                )
            }
        )
        assert result.get(output, as_type=pd.DataFrame).shape[0] == 4


@with_instances("mssql")
def test_external_table_inputs_rawsql():
    @materialize(version="1.1")
    def make_external_table():
        table_store = ConfigContext.get().store.table_store
        schema = Schema("external_schema", prefix="", suffix="")
        table_name = "external_table"
        table_store.execute(CreateSchema(schema, if_not_exists=True))
        table_store.execute(DropTable(table_name, schema, if_exists=True))
        query = sql_table_expr({"col": [0, 1, 2, 3]})
        cmds = (
            [CreateTableAsSelect, InsertIntoSelect]
            if table_store.engine.dialect.name == "ibm_db_sa"
            else [CreateTableAsSelect]
        )
        for cmd in cmds:
            table_store.execute(
                cmd(
                    table_name,
                    schema,
                    query,
                )
            )
        return Table(ExternalTableReference(table_name, schema=schema.get()))

    @materialize(input_type=sa.Table)
    def duplicate_table_reference():
        sql = """
        SELECT 1 as col INTO sql_table_linked__tmp.duplicate_table_reference
        """
        return RawSql(sql)

    @materialize(input_type=pd.DataFrame)
    def identity(table: pd.DataFrame):
        return table

    with Flow() as f:
        with Stage("sql_table_origin"):
            _ = make_external_table()

        with Stage("sql_table_linked"):
            table = duplicate_table_reference()["duplicate_table_reference"]
            output = identity(table)

    with StageLockContext():
        # Normal execution. duplicate_table_reference should return an empty table
        result = f.run()
        assert result.get(output, as_type=pd.DataFrame).shape[0] == 1

    with StageLockContext():
        # Linked execution. Body of duplicate_table_reference should not be executed,
        # instead output is referenced from the linked table
        result = f.run(
            inputs={
                table: ExternalTableReference(
                    "external_table",
                    schema="external_schema",
                )
            }
        )
        assert result.get(output, as_type=pd.DataFrame).shape[0] == 4


def test_external_table_inputs_nout():
    @materialize(version="1.1")
    def make_external_table():
        table_store = ConfigContext.get().store.table_store
        schema = Schema("external_schema", prefix="", suffix="")
        table_name = "external_table"
        table_store.execute(CreateSchema(schema, if_not_exists=True))
        table_store.execute(DropTable(table_name, schema, if_exists=True))
        query = sql_table_expr({"col": [0, 1, 2, 3]})
        cmds = (
            [CreateTableAsSelect, InsertIntoSelect]
            if table_store.engine.dialect.name == "ibm_db_sa"
            else [CreateTableAsSelect]
        )
        for cmd in cmds:
            table_store.execute(
                cmd(
                    table_name,
                    schema,
                    query,
                )
            )
        return Table(ExternalTableReference(table_name, schema=schema.get()))

    @materialize(input_type=sa.Table, nout=2)
    def duplicate_table_reference():
        return Table(pd.DataFrame([1])), Table(pd.DataFrame())

    @materialize(input_type=pd.DataFrame)
    def identity(table: pd.DataFrame):
        return table

    with Flow() as f:
        with Stage("sql_table_origin"):
            _ = make_external_table()

        with Stage("sql_table_linked"):
            table, dummy = duplicate_table_reference()
            output = identity(table)

    with StageLockContext():
        # Normal execution. duplicate_table_reference should return an empty table
        result = f.run()
        assert result.get(output, as_type=pd.DataFrame).shape[0] == 1

    with StageLockContext():
        # Linked execution. Body of duplicate_table_reference should not be executed,
        # instead output is referenced from the linked table
        result = f.run(
            inputs={
                table: ExternalTableReference(
                    "external_table",
                    schema="external_schema",
                ),
                dummy: ExternalTableReference(
                    "external_table",
                    schema="external_schema",
                ),
            }
        )
        assert result.get(output, as_type=pd.DataFrame).shape[0] == 4
