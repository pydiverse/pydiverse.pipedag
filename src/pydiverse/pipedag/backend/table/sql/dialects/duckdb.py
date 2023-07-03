from __future__ import annotations

from pydiverse.pipedag.backend.table.sql.hooks import IbisTableHook
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore


class DuckDBTableStore(SQLTableStore):
    _dialect_name = "duckdb"

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
