# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import csv
from dataclasses import dataclass
from io import BytesIO, StringIO
from typing import Any

import pandas as pd
import sqlalchemy as sa

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
    Materialization details specific to PostgreSQL.

    :param unlogged: Whether to use `unlogged`_ tables or not.
        This reduces safety in case of a crash or unclean shutdown, but can
        significantly increase write performance.

    .. _unlogged:
        https://www.postgresql.org/docs/9.5/sql-createtable.html
        #SQL-CREATETABLE-UNLOGGED

    Example
    -------

    .. code-block:: yaml

        materialization_details:
            __any__:
                unlogged: true
            my_label:
                unlogged: false

    For more general information on materialization details, see
    ``materialization_details`` parameter of
    :py:class:`SQLTableStore <pydiverse.pipedag.backend.table.SQLTableStore>`.
    """

    def __post_init__(self):
        assert isinstance(self.unlogged, bool)

    unlogged: bool = False


class PostgresTableStore(SQLTableStore):
    """
    SQLTableStore that supports `PostgreSQL <https://postgresql.org>`_.

    Takes the same arguments as
    :py:class:`SQLTableStore <pydiverse.pipedag.backend.table.SQLTableStore>`.

    Supports the :py:class:`PostgresMaterializationDetails\
    <pydiverse.pipedag.backend.table.sql.dialects.postgres.PostgresMaterializationDetails>`
    materialization details.
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

    def lock_table(self, table: Table | str, schema: Schema | str, conn: Any = None) -> list:
        """
        For some dialects, it might be beneficial to lock a table before writing to it.
        """
        stmt = LockTable(table.name if isinstance(table, Table) else table, schema)
        if conn is not None:
            self.execute(stmt, conn=conn)
        return [stmt]

    def lock_source_table(self, table: Table | str, schema: Schema | str, conn: Any = None) -> list:
        """
        For some dialects, it might be beneficial to lock source tables before reading.
        """
        stmt = LockSourceTable(table.name if isinstance(table, Table) else table, schema)
        if conn is not None:
            self.execute(stmt, conn=conn)
        return [stmt]

    def check_materialization_details_supported(self, label: str | None) -> None:
        _ = label
        return

    def _set_materialization_details(self, materialization_details: dict[str, dict[str | list[str]]] | None) -> None:
        self.materialization_details = PostgresMaterializationDetails.create_materialization_details_dict(
            materialization_details,
            self.strict_materialization_details,
            self.default_materialization_details,
            self.logger,
        )


@PostgresTableStore.register_table()
class SQLAlchemyTableHook(hooks.SQLAlchemyTableHook):
    pass  # postges is our reference dialect


@PostgresTableStore.register_table(pd)
class PandasTableHook(hooks.PandasTableHook):
    @classmethod
    def _execute_materialize(
        cls,
        table: Table[pd.DataFrame],
        store: PostgresTableStore,
        schema: Schema,
        dtypes: dict[str, Dtype],
    ):
        df = table.obj
        dtypes = cls._get_dialect_dtypes(dtypes, table)

        # Create empty table
        cls._dialect_create_empty_table(store, table, schema, dtypes)
        store.add_indexes_and_set_nullable(table, schema, on_empty_table=True, table_cols=df.columns)

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
                sql = f"COPY {schema_name}.{table_name} FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
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
    def upload_table(
        cls,
        table: Table[pl.DataFrame],
        schema: Schema,
        dtypes: dict[str, sa.types.TypeEngine],
        store: SQLTableStore,
        early: bool,
    ):
        if not early:
            cls._dialect_create_empty_table(store, table, schema, dtypes)
        df = table.obj  # type: pl.DataFrame

        # COPY data
        # TODO: For python 3.12, there is csv.QUOTE_STRINGS
        #       This would make everything a bit safer, because then we could
        #       represent the string "\\N" (backslash + capital n).
        s_buf = BytesIO()
        df.write_csv(
            s_buf,
            null_value="\\N",
            include_header=False,
            quote_style="necessary",
        )
        s_buf.seek(0)

        engine = store.engine
        table_name = engine.dialect.identifier_preparer.quote(table.name)
        schema_name = engine.dialect.identifier_preparer.format_schema(schema.get())

        dbapi_conn = engine.raw_connection()
        try:
            with dbapi_conn.cursor() as cur:
                sql = f"COPY {schema_name}.{table_name} FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
                store.logger.info("Executing bulk load", query=sql)
                cur.copy_expert(sql=sql, file=s_buf)
            dbapi_conn.commit()
        finally:
            dbapi_conn.close()
