from __future__ import annotations

import csv
from dataclasses import dataclass
from io import StringIO
from typing import Any

import pandas as pd
import structlog

from pydiverse.common import Dtype
from pydiverse.pipedag.backend.table.sql import hooks
from pydiverse.pipedag.backend.table.sql.ddl import (
    ChangeTableLogged,
    LockSourceTable,
    LockTable,
)
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore
from pydiverse.pipedag.container import Schema, Table
from pydiverse.pipedag.materialize.details import (
    BaseMaterializationDetails,
    resolve_materialization_details_label,
)

try:
    import polars as pl
except ImportError:
    pl = None

try:
    import pyarrow as pa
except ImportError:
    pa = None


@dataclass(frozen=True)
class PostgresMaterializationDetails(BaseMaterializationDetails):
    """
    :param unlogged: Whether to use `unlogged`_ tables or not.
        This reduces safety in case of a crash or unclean shutdown, but can
        significantly increase write performance.

    .. _unlogged:
        https://www.postgresql.org/docs/9.5/sql-createtable.html
        #SQL-CREATETABLE-UNLOGGED
    """

    def __post_init__(self):
        assert isinstance(self.unlogged, bool)

    unlogged: bool = False


class PostgresTableStore(SQLTableStore):
    """
    SQLTableStore that supports `PostgreSQL <https://postgresql.org>`_.

    Takes the same arguments as
    :py:class:`SQLTableStore <pydiverse.pipedag.backend.table.SQLTableStore>`
    """

    _dialect_name = "postgresql"

    def __init__(self, *args, use_adbc: bool = True, **kwargs):
        self.use_adbc = use_adbc
        super().__init__(*args, **kwargs)

    def _init_database(self):
        self._init_database_with_database("postgres", {"isolation_level": "AUTOCOMMIT"})

    def get_unlogged(self, materialization_details_label: str | None) -> bool:
        return PostgresMaterializationDetails.get_attribute_from_dict(
            self.materialization_details,
            materialization_details_label,
            self.default_materialization_details,
            "unlogged",
            self.strict_materialization_details,
            self.logger,
        )

    def lock_table(
        self, table: Table | str, schema: Schema | str, conn: Any = None
    ) -> list:
        """
        For some dialects, it might be beneficial to lock a table before writing to it.
        """
        stmt = LockTable(table.name if isinstance(table, Table) else table, schema)
        if conn is not None:
            self.execute(stmt, conn=conn)
        return [stmt]

    def lock_source_table(
        self, table: Table | str, schema: Schema | str, conn: Any = None
    ) -> list:
        """
        For some dialects, it might be beneficial to lock source tables before reading.
        """
        stmt = LockSourceTable(
            table.name if isinstance(table, Table) else table, schema
        )
        if conn is not None:
            self.execute(stmt, conn=conn)
        return [stmt]

    def check_materialization_details_supported(self, label: str | None) -> None:
        _ = label
        return

    def _set_materialization_details(
        self, materialization_details: dict[str, dict[str | list[str]]] | None
    ) -> None:
        self.materialization_details = (
            PostgresMaterializationDetails.create_materialization_details_dict(
                materialization_details,
                self.strict_materialization_details,
                self.default_materialization_details,
                self.logger,
            )
        )


@PostgresTableStore.register_table()
class SQLAlchemyTableHook(hooks.SQLAlchemyTableHook):
    pass  # postges is our reference dialect


@PostgresTableStore.register_table(pd)
class PandasTableHook(hooks.PandasTableHook):
    @classmethod
    def _execute_materialize(
        cls,
        df: pd.DataFrame,
        store: PostgresTableStore,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, Dtype],
    ):
        dtypes = {name: dtype.to_sql() for name, dtype in dtypes.items()}
        if table.type_map:
            dtypes.update(table.type_map)

        # Create empty table
        cls._dialect_create_empty_table(store, df, table, schema, dtypes)
        store.add_indexes_and_set_nullable(
            table, schema, on_empty_table=True, table_cols=df.columns
        )

        if store.get_unlogged(resolve_materialization_details_label(table)):
            store.execute(ChangeTableLogged(table.name, schema, False))

        # COPY data
        # TODO: For python 3.12, there is csv.QUOTE_STRINGS
        #       This would make everything a bit safer, because then we could
        #       represent the string "\\N" (backslash + capital n).
        s_buf = StringIO()
        df.to_csv(
            s_buf,
            na_rep="\\N",
            header=False,
            index=False,
            quoting=csv.QUOTE_MINIMAL,
        )
        s_buf.seek(0)

        engine = store.engine
        table_name = engine.dialect.identifier_preparer.quote(table.name)
        schema_name = engine.dialect.identifier_preparer.format_schema(schema.get())

        dbapi_conn = engine.raw_connection()
        try:
            with dbapi_conn.cursor() as cur:
                sql = (
                    f"COPY {schema_name}.{table_name} FROM STDIN"
                    " WITH (FORMAT CSV, NULL '\\N')"
                )
                store.logger.info("Executing bulk load", query=sql)
                cur.copy_expert(sql=sql, file=s_buf)
            dbapi_conn.commit()
        finally:
            dbapi_conn.close()
        store.add_indexes_and_set_nullable(
            table,
            schema,
            on_empty_table=False,
        )


@PostgresTableStore.register_table(pl)
class PolarsTableHook(hooks.PolarsTableHook):
    @classmethod
    def materialize(
        cls, store: PostgresTableStore, table: Table[pl.DataFrame], stage_name: str
    ):
        if not store.use_adbc:
            super().materialize(store, table, stage_name)

        import adbc_driver_postgresql.dbapi

        df = table.obj
        try:
            df = cls._apply_materialize_annotation(df, table)
        except Exception as e:
            logger = structlog.get_logger(logger_name=cls.__name__)
            logger.error(
                "Failed to apply materialize annotation for table %s: %s",
                table.name,
                e,
            )

        schema = store.get_schema(stage_name)
        dtypes = {
            name: Dtype.from_polars(dtype).to_sql()
            for name, dtype in df.collect_schema().items()
        }

        if table.type_map:
            dtypes.update(table.type_map)

        engine = store.engine
        table_name = engine.dialect.identifier_preparer.quote(table.name)
        schema_name = engine.dialect.identifier_preparer.format_schema(schema.get())

        conn = adbc_driver_postgresql.dbapi.connect(
            engine.url.render_as_string(hide_password=False)
        )

        # Create empty table
        df.slice(0, 0).write_database(
            f"{schema_name}.{table_name}",
            conn,
            if_table_exists="replace",
            engine="adbc",
        )
        store.add_indexes_and_set_nullable(
            table, schema, on_empty_table=True, table_cols=df.columns
        )

        if store.get_unlogged(resolve_materialization_details_label(table)):
            store.execute(ChangeTableLogged(table.name, schema, False))

        df.write_database(
            f"{schema_name}.{table_name}",
            conn,
            if_table_exists="append",
            engine="adbc",
        )
