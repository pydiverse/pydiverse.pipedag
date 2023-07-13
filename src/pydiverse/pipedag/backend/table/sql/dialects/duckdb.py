from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa

from pydiverse.pipedag.backend.table.sql.hooks import IbisTableHook
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore


class DuckDBTableStore(SQLTableStore):
    """
    SQLTableStore that supports `DuckDB <https://duckdb.org>`_.

    Takes the same arguments as
    :py:class:`SQLTableStore <pydiverse.pipedag.backend.table.SQLTableStore>`
    """

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
        if not self.create_database_if_not_exists:
            return

        # Duckdb already creates the database file automatically
        # However, the parent directory doesn't get created automatically
        database = self.engine_url.database
        if database == ":memory:":
            return

        database_path = Path(database)
        database_path.parent.mkdir(parents=True, exist_ok=True)


try:
    import ibis
except ImportError:
    ibis = None


@DuckDBTableStore.register_table(ibis)
class IbisTableHook(IbisTableHook):
    @classmethod
    def _conn(cls, store: DuckDBTableStore):
        return ibis.duckdb._from_url(store.engine_url)
