from __future__ import annotations

import re
from typing import Any

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.dialects.mssql

from pydiverse.pipedag.backend.table.sql.ddl import (
    AddIndex,
    AddPrimaryKey,
    ChangeColumnTypes,
    CreateAlias,
    Schema,
    _mssql_update_definition,
)
from pydiverse.pipedag.backend.table.sql.hooks import IbisTableHook, PandasTableHook
from pydiverse.pipedag.backend.table.sql.reflection import PipedagMSSqlReflection
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore
from pydiverse.pipedag.backend.table.util import DType
from pydiverse.pipedag.materialize import Table
from pydiverse.pipedag.materialize.container import RawSql


class MSSqlTableStore(SQLTableStore):
    _dialect_name = "mssql"

    def __init__(
        self,
        *args,
        disable_pytsql: bool = False,
        pytsql_isolate_top_level_statements: bool = True,
        **kwargs,
    ):
        """
        :param disable_pytsql: whether to disable the use of pytsql
        :param pytsql_isolate_top_level_statements: forward pytsql executes() parameter
        """
        self.disable_pytsql = disable_pytsql
        self.pytsql_isolate_top_level_statements = pytsql_isolate_top_level_statements

        super().__init__(*args, **kwargs)

    def _init_database(self):
        self._init_database_with_database("master", {"isolation_level": "AUTOCOMMIT"})

    def add_primary_key(
        self,
        table_name: str,
        schema: Schema,
        key_columns: list[str],
        *,
        name: str | None = None,
        early_not_null_possible: bool = False,
    ):
        _ = early_not_null_possible  # not needed for this dialect
        sql_types = self.reflect_sql_types(key_columns, table_name, schema)
        # impose some varchar(max) limit to allow use in primary key / index
        # TODO: consider making cap_varchar_max a config option
        self.execute(
            ChangeColumnTypes(
                table_name,
                schema,
                key_columns,
                sql_types,
                nullable=False,
                cap_varchar_max=1024,
            )
        )
        self.execute(AddPrimaryKey(table_name, schema, key_columns, name))

    def add_index(
        self,
        table_name: str,
        schema: Schema,
        index_columns: list[str],
        name: str | None = None,
    ):
        sql_types = self.reflect_sql_types(index_columns, table_name, schema)
        if any(
            [
                isinstance(_type, sa.String) and _type.length is None
                for _type in sql_types
            ]
        ):
            # impose some varchar(max) limit to allow use in primary key / index
            self.execute(
                ChangeColumnTypes(
                    table_name, schema, index_columns, sql_types, cap_varchar_max=1024
                )
            )
        self.execute(AddIndex(table_name, schema, index_columns, name))

    def execute_raw_sql(self, raw_sql: RawSql):
        if self.disable_pytsql:
            self.__execute_raw_sql_fallback(raw_sql)
        else:
            self.__execute_raw_sql_pytsql(raw_sql)

    def __execute_raw_sql_fallback(self, raw_sql: RawSql):
        """Alternative to using pytsql for splitting SQL statement.

        Known Problems:
        - DECLARE statements will not be available across GO
        """
        last_use_statement = None
        for statement in re.split(r"\bGO\b", raw_sql.sql, flags=re.IGNORECASE):
            statement = statement.strip()
            if statement == "":
                # allow GO at end of script
                continue

            if re.match(r"\bUSE\b", statement):
                last_use_statement = statement

            with self.engine_connect() as conn:
                if last_use_statement is not None:
                    self.execute(last_use_statement, conn=conn)
                self.execute(statement, conn=conn)

    def __execute_raw_sql_pytsql(self, raw_sql: RawSql):
        """Use pytsql for executing T-SQL scripts containing many GO statements."""
        # noinspection PyUnresolvedReferences,PyPackageRequirements
        import pytsql

        sql_string = raw_sql.sql
        if self.print_sql:
            print_query_string = self.format_sql_string(sql_string)
            max_length = 5000
            if len(print_query_string) >= max_length:
                print_query_string = print_query_string[:max_length] + " [...]"
            self.logger.info("Executing sql", query=print_query_string)

        pytsql.executes(
            sql_string,
            self.engine,
            isolate_top_level_statements=self.pytsql_isolate_top_level_statements,
        )

    def _get_all_objects_in_schema(self, schema: Schema) -> dict[str, Any]:
        with self.engine_connect() as conn:
            # tables (U), views (V), synonyms (SN), procedures (P) and functions (FN)
            query = """
                SELECT obj.name, obj.type
                FROM sys.objects AS obj
                LEFT JOIN sys.schemas AS schem ON schem.schema_id = obj.schema_id
                WHERE obj.type IN ('U', 'V', 'SN', 'P', 'FN')
                  AND schem.name = :schema
                  AND obj.is_ms_shipped = 0
            """
            result = conn.execute(sa.text(query), {"schema": schema.get()}).all()
        return {name: type_.strip() for name, type_ in result}

    def _copy_object_to_transaction(
        self,
        name: str,
        metadata: Any,
        src_schema: Schema,
        dest_schema: Schema,
        conn: sa.Connection,
    ):
        type_: str = metadata

        if type_ == "SN":
            alias_name, alias_schema = self.resolve_alias(name, src_schema.get())
            self.execute(
                CreateAlias(
                    alias_name,
                    Schema(alias_schema, "", ""),
                    name,
                    dest_schema,
                )
            )
            return

        # For views, procedures and functions we need to inspect the definition
        # of the object and replace all references to the src schema with
        # references to the dest schema.
        assert type_ in ("V", "P", "FN")
        definition = _mssql_update_definition(conn, name, src_schema, dest_schema)
        self.execute(definition, conn=conn)

    def resolve_alias(self, table: str, schema: str):
        return PipedagMSSqlReflection.resolve_alias(self.engine, table, schema)


@MSSqlTableStore.register_table(pd)
class PandasTableHook(PandasTableHook):
    @classmethod
    def _execute_materialize(
        cls,
        df: pd.DataFrame,
        store: MSSqlTableStore,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, DType],
    ):
        dtypes = ({name: dtype.to_sql() for name, dtype in dtypes.items()}) | (
            {
                name: sa.dialects.mssql.DATETIME2()
                for name, dtype in dtypes.items()
                if dtype == DType.DATETIME
            }
        )

        if table.type_map:
            dtypes.update(table.type_map)

        df.to_sql(
            table.name,
            store.engine,
            schema=schema.get(),
            index=False,
            dtype=dtypes,
            chunksize=100_000,
        )


try:
    import ibis
except ImportError:
    ibis = None


@MSSqlTableStore.register_table(ibis)
class IbisTableHook(IbisTableHook):
    @classmethod
    def _conn(cls, store: MSSqlTableStore):
        url = store.engine_url
        return ibis.mssql.connect(
            host=url.host,
            user=url.username,
            password=url.password,
            port=url.port,
            database=url.database,
        )
