# register all SQLTableStore dialects (otherwise, they cannot be selected by DB URL)
from __future__ import annotations

from .duckdb import DuckDBTableStore
from .ibm_db2 import IBMDB2TableStore
from .mssql import MSSqlTableStore
from .postgres import PostgresTableStore
from .snowflake import SnowflakeTableStore

__all__ = [
    "DuckDBTableStore",
    "IBMDB2TableStore",
    "MSSqlTableStore",
    "PostgresTableStore",
    "SnowflakeTableStore",
]
