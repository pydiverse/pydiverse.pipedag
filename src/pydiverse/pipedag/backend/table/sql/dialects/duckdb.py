# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause
import importlib
import warnings
from pathlib import Path
from typing import Any

import pandas as pd
import sqlalchemy as sa

from pydiverse.common import Dtype
from pydiverse.pipedag import Table
from pydiverse.pipedag.backend.table.sql.hooks import (
    IbisTableHook,
    PandasTableHook,
    PolarsTableHook,
)
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore
from pydiverse.pipedag.container import Schema
from pydiverse.pipedag.materialize.details import resolve_materialization_details_label

try:
    import duckdb
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    duckdb = None


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

    def _default_isolation_level(self) -> str | None:
        return None  # "READ UNCOMMITTED" does not exist in DuckDB

    def _init_database_before_engine(self):
        # Duckdb already creates the database file automatically
        # However, the parent directory doesn't get created automatically
        database = self.engine_url.database
        if database is None or database == ":memory:":
            return

        database_path = Path(database)
        database_path.parent.mkdir(parents=True, exist_ok=True)

    def _init_database(self):
        if not self.create_database_if_not_exists:
            return

    def dialect_requests_empty_creation(self, table: Table, is_sql: bool) -> bool:
        _ = table, is_sql
        return False  # DuckDB is not good with stable type arithmetic


@DuckDBTableStore.register_table(pd)
class PandasTableHook(PandasTableHook):
    @classmethod
    def _execute_materialize(
        cls,
        table: Table[pd.DataFrame],
        store: DuckDBTableStore,
        schema: Schema,
        dtypes: dict[str, Dtype],
    ):
        df = table.obj
        engine = store.engine
        dtypes = cls._get_dialect_dtypes(dtypes, table)

        store.check_materialization_details_supported(resolve_materialization_details_label(table))

        # Create empty table with correct schema
        cls._dialect_create_empty_table(store, table, schema, dtypes)
        store.add_indexes_and_set_nullable(table, schema, on_empty_table=True, table_cols=df.columns)

        # Copy dataframe directly to duckdb
        # This is SIGNIFICANTLY faster than using pandas.to_sql
        table_name = engine.dialect.identifier_preparer.quote(table.name)
        schema_name = engine.dialect.identifier_preparer.format_schema(schema.get())

        conn = engine.raw_connection()
        # Attention: This sql copies local variable df into database (FROM df)
        conn.execute(f"INSERT INTO {schema_name}.{table_name} SELECT * FROM df")

        store.add_indexes_and_set_nullable(
            table,
            schema,
            on_empty_table=False,
            table_cols=df.columns,
        )


try:
    import polars as pl
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    pl = importlib.import_module("polars")
    pl.DataType = None
    pl.DataFrame = None
    pl.LazyFrame = None


@DuckDBTableStore.register_table(pl.DataFrame, duckdb)
class PolarsTableHook(PolarsTableHook):
    @classmethod
    def download_table(
        cls,
        query: Any,
        store: SQLTableStore,
        dtypes: dict[str, pl.DataType] | None = None,
    ) -> pl.DataFrame:
        assert dtypes is None, (
            "Polars reads SQL schema and loads the data in reasonable types."
            "Thus, manual dtype manipulation can only done via query or afterwards."
        )
        engine = store.engine
        # Connectorx doesn't support duckdb.
        # Instead, we load it like this:  DuckDB -> PyArrow -> Polars
        conn = engine.raw_connection()
        pl_table = conn.sql(query).arrow()

        df = pl.from_arrow(pl_table)
        return df


try:
    import ibis
except ImportError:
    ibis = None


@DuckDBTableStore.register_table(ibis)
class IbisTableHook(IbisTableHook):
    @classmethod
    def _conn(cls, store: DuckDBTableStore):
        return ibis.duckdb.from_connection(store.engine.raw_connection())
