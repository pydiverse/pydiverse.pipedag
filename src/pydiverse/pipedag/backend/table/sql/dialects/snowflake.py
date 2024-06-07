from __future__ import annotations

import warnings

from pydiverse.pipedag.backend.table.sql.hooks import (
    IbisTableHook,
)
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore

try:
    import snowflake
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    snowflake = None


class SnowflakeTableStore(SQLTableStore):
    """
    SQLTableStore that supports `Snowflake`_.

    Takes the same arguments as
    :py:class:`SQLTableStore <pydiverse.pipedag.backend.table.SQLTableStore>`
    """

    _dialect_name = "snowflake"

    def _default_isolation_level(self) -> str | None:
        return None  # "READ UNCOMMITTED" does not exist in DuckDB

    def _init_database(self):
        create_database = self.engine_url.database.split("/")[0]
        with self.engine.connect() as conn:
            if not [
                x.name
                for x in conn.exec_driver_sql("SHOW DATABASES").mappings().all()
                if x.name.upper() == create_database.upper()
            ]:
                self._init_database_with_database(
                    "snowflake",
                    disable_exists_check=True,
                    create_database=create_database,
                )


# @SnowflakeTableStore.register_table(pd)
# class PandasTableHook(PandasTableHook):
#     @classmethod
#     def _execute_materialize(
#         cls,
#         df: pd.DataFrame,
#         store: SnowflakeTableStore,
#         table: Table[pd.DataFrame],
#         schema: Schema,
#         dtypes: dict[str, DType],
#     ):
#         engine = store.engine
#         dtypes = cls._get_dialect_dtypes(dtypes, table)
#         if table.type_map:
#             dtypes.update(table.type_map)
#
#         store.check_materialization_details_supported(
#             resolve_materialization_details_label(table)
#         )
#
#         # Create empty table with correct schema
#         cls._dialect_create_empty_table(store, df, table, schema, dtypes)
#         store.add_indexes_and_set_nullable(
#             table, schema, on_empty_table=True, table_cols=df.columns
#         )
#
#         # Copy dataframe directly to duckdb
#         # This is SIGNIFICANTLY faster than using pandas.to_sql
#         table_name = engine.dialect.identifier_preparer.quote(table.name)
#         schema_name = engine.dialect.identifier_preparer.format_schema(schema.get())
#
#         connection_uri = store.engine_url.render_as_string(hide_password=False)
#         connection_uri = connection_uri.replace("duckdb:///", "", 1)
#         with duckdb.connect(connection_uri) as conn:
#             conn.execute(f"INSERT INTO {schema_name}.{table_name} SELECT * FROM df")
#
#         store.add_indexes_and_set_nullable(
#             table,
#             schema,
#             on_empty_table=False,
#             table_cols=df.columns,
#         )


try:
    import polars
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    polars = None


# @SnowflakeTableStore.register_table(polars, snowflake)
# class PolarsTableHook(PolarsTableHook):
#     @classmethod
#     def _execute_query(cls, query: str, connection_uri: str, store: SQLTableStore):
#         # Connectorx doesn't support duckdb.
#         # Instead, we load it like this:  Snowflake -> PyArrow -> Polars
#         connection_uri = connection_uri.replace("snowflake:///", "", 1)
#         with snowflake.connect(connection_uri) as conn:
#             pl_table = conn.sql(query).arrow()
#
#         df = polars.from_arrow(pl_table)
#         return df


try:
    import ibis
except ImportError:
    ibis = None


@SnowflakeTableStore.register_table(ibis)
class IbisTableHook(IbisTableHook):
    @classmethod
    def _conn(cls, store: SnowflakeTableStore):
        return ibis.snowflake._from_url(store.engine_url)
