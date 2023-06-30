from __future__ import annotations

import time

import sqlalchemy as sa
import sqlalchemy.exc

from pydiverse.pipedag.backend.table.sql.ddl import (
    AddPrimaryKey,
    ChangeColumnNullable,
    Schema,
)
from pydiverse.pipedag.backend.table.sql.reflection import PipedagDB2Reflection
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore


class IBMDB2TableStore(SQLTableStore):
    _dialect_name = "ibm_db_sa"

    def add_primary_key(
        self,
        table_name: str,
        schema: Schema,
        key_columns: list[str],
        *,
        name: str | None = None,
        early_not_null_possible: bool = False,
    ):
        if not early_not_null_possible:
            for retry_iteration in range(4):
                # retry operation since it might have been terminated as a
                # deadlock victim
                try:
                    self.execute(
                        ChangeColumnNullable(
                            table_name, schema, key_columns, nullable=False
                        )
                    )
                    break
                except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError):
                    if retry_iteration == 3:
                        raise
                    time.sleep(retry_iteration * retry_iteration * 1.1)
        self.execute(AddPrimaryKey(table_name, schema, key_columns, name))

    def resolve_alias(self, table, schema):
        return PipedagDB2Reflection.resolve_alias(self.engine, table, schema)
