from __future__ import annotations

from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore


class DuckDBTableStore(SQLTableStore):
    _dialect_name = "duckdb"

    def _init_database(self):
        # Duckdb already creates the database file automatically
        pass
