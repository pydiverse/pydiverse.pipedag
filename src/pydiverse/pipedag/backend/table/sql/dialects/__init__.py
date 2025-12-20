# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

# register all SQLTableStore dialects (otherwise, they cannot be selected by DB URL)
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
