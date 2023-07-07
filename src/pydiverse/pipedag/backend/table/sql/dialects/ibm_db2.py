from __future__ import annotations

import time

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc

from pydiverse.pipedag.backend.table.sql.ddl import (
    AddPrimaryKey,
    ChangeColumnNullable,
    Schema,
)
from pydiverse.pipedag.backend.table.sql.hooks import PandasTableHook
from pydiverse.pipedag.backend.table.sql.reflection import PipedagDB2Reflection
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore
from pydiverse.pipedag.backend.table.util import DType
from pydiverse.pipedag.materialize import Table


class IBMDB2TableStore(SQLTableStore):
    """
    SQLTableStore that supports `IBM Db2 <https://www.ibm.com/products/db2>`_.
    Requires `ibm-db-sa <https://pypi.org/project/ibm-db-sa/>`_ to be installed.

    Takes the same arguments as
    :py:class:`SQLTableStore <pydiverse.pipedag.backend.table.SQLTableStore>`
    """

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


@IBMDB2TableStore.register_table(pd)
class PandasTableHook(PandasTableHook):
    @classmethod
    def _execute_materialize(
        cls,
        df: pd.DataFrame,
        store: SQLTableStore,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, DType],
    ):
        # Default string target is CLOB which can't be used for indexing.
        # -> Convert indexed string columns to VARCHAR(256)
        index_columns = set()
        if indexes := table.indexes:
            index_columns |= {col for index in indexes for col in index}
        if primary_key := table.primary_key:
            index_columns |= set(primary_key)

        dtypes = ({name: dtype.to_sql() for name, dtype in dtypes.items()}) | (
            {
                name: (
                    sa.String(length=256)
                    if name in index_columns
                    else sa.String(length=32_672)
                )
                for name, dtype in dtypes.items()
                if dtype == DType.STRING
            }
        )

        if table.type_map:
            dtypes.update(table.type_map)

        df.to_sql(
            table.name,
            store.engine,
            schema=schema.get(),
            index=False,
            dtype=dtypes,
            chunksize=100_000,
        )
