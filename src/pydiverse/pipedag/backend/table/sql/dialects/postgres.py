from __future__ import annotations

from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore


class PostgresTableStore(SQLTableStore):
    _dialect_name = "postgresql"

    def _init_database(self):
        self._init_database_with_database("postgres", {"isolation_level": "AUTOCOMMIT"})
