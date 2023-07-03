from __future__ import annotations

import sqlalchemy as sa

from pydiverse.pipedag.backend.table.sql.hooks import IbisTableHook
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore


class DuckDBTableStore(SQLTableStore):
    _dialect_name = "duckdb"

    def _metadata_pk(self, name: str, table_name: str):
        sequence = sa.Sequence(f"{table_name}_{name}_seq", metadata=self.sql_metadata)
        return sa.Column(
            name,
            sa.BigInteger(),
            sequence,
            server_default=sequence.next_value(),
            primary_key=True,
        )

    def _init_database(self):
        # Duckdb already creates the database file automatically
        pass


try:
    import ibis
except ImportError:
    ibis = None


@DuckDBTableStore.register_table(ibis)
class IbisTableHook(IbisTableHook):
    @classmethod
    def _conn(cls, store: DuckDBTableStore):
        return ibis.duckdb._from_url(store.engine_url)
