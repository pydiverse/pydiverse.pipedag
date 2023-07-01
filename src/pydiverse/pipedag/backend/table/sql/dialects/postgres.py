from __future__ import annotations

import csv
from io import StringIO

import pandas as pd

from pydiverse.pipedag.backend.table.sql.ddl import ChangeTableLogged, Schema
from pydiverse.pipedag.backend.table.sql.hooks import PandasTableHook
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore
from pydiverse.pipedag.backend.table.util import DType
from pydiverse.pipedag.materialize import Table


class PostgresTableStore(SQLTableStore):
    _dialect_name = "postgresql"

    def _init_database(self):
        self._init_database_with_database("postgres", {"isolation_level": "AUTOCOMMIT"})


@PostgresTableStore.register_table(pd)
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
        engine = store.engine
        dtypes = {name: dtype.to_sql() for name, dtype in dtypes.items()}
        if table.type_map:
            dtypes.update(table.type_map)

        # Create empty table
        df[:0].to_sql(
            table.name,
            engine,
            schema=schema.get(),
            index=False,
            dtype=dtypes,
        )

        if store.unlogged_tables:
            with engine.connect() as conn:
                conn.execute(ChangeTableLogged(table.name, schema, False))

        # COPY data
        # TODO: For python 3.12, there is csv.QUOTE_STRINGS
        #       This would make everything a bit safer, because then we could represent
        #       the string "\\N" (backslash + capital n).
        s_buf = StringIO()
        df.to_csv(
            s_buf,
            na_rep="\\N",
            header=False,
            index=False,
            quoting=csv.QUOTE_MINIMAL,
        )
        s_buf.seek(0)

        dbapi_conn = engine.raw_connection()
        try:
            with dbapi_conn.cursor() as cur:
                sql = (
                    f"COPY {schema.get()}.{table.name} FROM STDIN"
                    " WITH (FORMAT CSV, NULL '\\N')"
                )
                cur.copy_expert(sql=sql, file=s_buf)
            dbapi_conn.commit()
        finally:
            dbapi_conn.close()
