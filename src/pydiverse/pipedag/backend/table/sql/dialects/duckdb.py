from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Table
from pydiverse.pipedag.backend.table.sql.hooks import (
    IbisTableHook,
    PandasTableHook,
    PolarsTableHook,
)
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore
from pydiverse.pipedag.backend.table.util import DType
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

    def dialect_requests_empty_creation(self, table: Table, is_sql: bool) -> bool:
        _ = table, is_sql
        return False  # DuckDB is not good with stable type arithmetic


@DuckDBTableStore.register_table(pd)
class PandasTableHook(PandasTableHook):
    @classmethod
    def _execute_materialize(
        cls,
        df: pd.DataFrame,
        store: DuckDBTableStore,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, DType],
    ):
        engine = store.engine
        dtypes = cls._get_dialect_dtypes(dtypes, table)
        if table.type_map:
            dtypes.update(table.type_map)

        store.check_materialization_details_supported(
            resolve_materialization_details_label(table)
        )

        # Create empty table with correct schema
        cls._dialect_create_empty_table(store, df, table, schema, dtypes)
        store.add_indexes_and_set_nullable(
            table, schema, on_empty_table=True, table_cols=df.columns
        )

        # Copy dataframe directly to duckdb
        # This is SIGNIFICANTLY faster than using pandas.to_sql
        table_name = engine.dialect.identifier_preparer.quote(table.name)
        schema_name = engine.dialect.identifier_preparer.format_schema(schema.get())

        connection_uri = store.engine_url.render_as_string(hide_password=False)
        connection_uri = connection_uri.replace("duckdb:///", "", 1)
        with duckdb.connect(connection_uri) as conn:
            conn.execute(f"INSERT INTO {schema_name}.{table_name} SELECT * FROM df")

        store.add_indexes_and_set_nullable(
            table,
            schema,
            on_empty_table=False,
            table_cols=df.columns,
        )


try:
    import polars
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    polars = None


@DuckDBTableStore.register_table(polars, duckdb)
class PolarsTableHook(PolarsTableHook):
    @classmethod
    def _execute_query(cls, query: str, connection_uri: str, store: SQLTableStore):
        # Connectorx doesn't support duckdb.
        # Instead, we load it like this:  DuckDB -> PyArrow -> Polars
        connection_uri = connection_uri.replace("duckdb:///", "", 1)
        with duckdb.connect(connection_uri) as conn:
            pl_table = conn.sql(query).arrow()

        df = polars.from_arrow(pl_table)
        return df


try:
    import ibis
except ImportError:
    ibis = None


@DuckDBTableStore.register_table(ibis)
class IbisTableHook(IbisTableHook):
    @classmethod
    def _conn(cls, store: DuckDBTableStore):
        return ibis.duckdb._from_url(str(store.engine_url))
