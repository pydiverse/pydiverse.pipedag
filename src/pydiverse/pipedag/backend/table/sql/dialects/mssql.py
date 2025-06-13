# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import dataclasses
import re
import types
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.dialects.mssql
from sqlalchemy import URL, Engine

from pydiverse.common import Datetime, Dtype
from pydiverse.pipedag.backend.table.sql.ddl import (
    ChangeColumnTypes,
    CreateAlias,
    _mssql_update_definition,
)
from pydiverse.pipedag.backend.table.sql.hooks import (
    IbisTableHook,
    PandasTableHook,
    PolarsTableHook,
)
from pydiverse.pipedag.backend.table.sql.reflection import PipedagMSSqlReflection
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore
from pydiverse.pipedag.container import RawSql, Schema, Table


class MSSqlTableStore(SQLTableStore):
    """
    SQLTableStore that supports `Microsoft SQL Server`_.

    In addition to the arguments of
    :py:class:`SQLTableStore <pydiverse.pipedag.backend.table.SQLTableStore>`,
    it also takes the following arguments:

    :param disable_pytsql:
        For mssql, a package called `pytsql <https://pypi.org/project/pytsql/>`_ is
        used for executing RawSql scripts. It has the advantage that it allows for
        some kind of SQL based print statements. However, it may fail for some
        statements. For those cases, you can set ``disable_pytsql: true`` to use another
        logic for splitting up Raw SQL scripts and handing that over to sqlalchemy.
        This is actually quite a complex process for mssql.
        Sorry for any inconveniences. We will try to make it work for most tsql code
        that should be integrated in pipedag pipelines. However, the ultimate goal
        is to split up monolithic blocks of dynamic sql statements into defined
        transformations with dynamic aspects written in python.

    :param pytsql_isolate_top_level_statements:
        This parameter is handed over to `pytsql.executes`_ and causes the script to
        be split in top level statements that are sent to sqlalchemy separately.
        The tricky part here is that some magic is done to make DECLARE statements
        reach across, but it is not guaranteed to be identical to scripts executed
        by a SQL UI.

    :param max_query_print_length:
        Maximum number of characters in a SQL query that are printed when
        logging the query before the log is truncated. Defaults to 5000 characters.

    .. _Microsoft SQL Server:
        https://www.microsoft.com/en/sql-server/
    .. _pytsql.executes:
        https://pytsql.readthedocs.io/en/latest/api/pytsql.html#pytsql.executes
    """

    _dialect_name = "mssql"

    def __init__(
        self,
        *args,
        disable_pytsql: bool = False,
        pytsql_isolate_top_level_statements: bool = True,
        max_query_print_length: int = 500000,
        **kwargs,
    ):
        self.disable_pytsql = disable_pytsql
        self.pytsql_isolate_top_level_statements = pytsql_isolate_top_level_statements
        self.max_query_print_length = max_query_print_length

        super().__init__(*args, **kwargs)

    def _init_database(self):
        self._init_database_with_database("master", {"isolation_level": "AUTOCOMMIT"})

    def dialect_requests_empty_creation(self, table: Table, is_sql: bool) -> bool:
        _ = is_sql
        return (
            table.nullable is not None
            or table.non_nullable is not None
            or (table.primary_key is not None and len(table.primary_key) > 0)
            or (table.indexes is not None and len(table.indexes) > 0)
        )

    def get_forced_nullability_columns(
        self, table: Table, table_cols: Iterable[str], report_nullable_cols=False
    ) -> tuple[list[str], list[str]]:
        # mssql dialect has literals as non-nullable types by default, so we also need
        # the list of nullable columns as well
        return self._process_table_nullable_parameters(table, table_cols)

    def add_indexes_and_set_nullable(
        self,
        table: Table,
        schema: Schema,
        *,
        on_empty_table: bool | None = None,
        table_cols: Iterable[str] | None = None,
    ):
        if on_empty_table is None or on_empty_table:
            # Set non-nullable and primary key on empty table
            key_columns = (
                [table.primary_key]
                if isinstance(table.primary_key, str)
                else table.primary_key
                if table.primary_key is not None
                else []
            )
            # reflect sql types because mssql cannot just change nullable flag
            inspector = sa.inspect(self.engine)
            columns = inspector.get_columns(table.name, schema=schema.get())
            table_cols = [d["name"] for d in columns]
            types = {d["name"]: d["type"] for d in columns}
            nullable_cols, non_nullable_cols = self.get_forced_nullability_columns(
                table, table_cols
            )
            non_nullable_cols = [
                col for col in non_nullable_cols if col not in key_columns
            ]
            sql_types = [types[col] for col in nullable_cols]
            if len(nullable_cols) > 0:
                self.execute(
                    ChangeColumnTypes(
                        table.name,
                        schema,
                        nullable_cols,
                        sql_types,
                        nullable=True,
                    )
                )
            sql_types = [types[col] for col in non_nullable_cols]
            if len(non_nullable_cols) > 0:
                self.execute(
                    ChangeColumnTypes(
                        table.name,
                        schema,
                        non_nullable_cols,
                        sql_types,
                        nullable=False,
                    )
                )
            if len(key_columns) > 0:
                sql_types = [types[col] for col in key_columns]
                # impose some varchar(max) limit to allow use in primary key / index
                # TODO: consider making cap_varchar_max a config option
                self.execute(
                    ChangeColumnTypes(
                        table.name,
                        schema,
                        key_columns,
                        sql_types,
                        nullable=False,
                        cap_varchar_max=1024,
                    )
                )
                index_columns = set()  # type: set[str]
                if table.indexes is not None:
                    for index in table.indexes:
                        index_columns |= set(index)
                index_columns_list = list(index_columns)
                sql_types = self.reflect_sql_types(
                    index_columns_list, table.name, schema
                )
                index_str_max_columns = [
                    col
                    for _type, col in zip(sql_types, index_columns_list)
                    if isinstance(_type, sa.String) and _type.length is None
                ]
                if len(index_str_max_columns) > 0:
                    # impose some varchar(max) limit to allow use in primary key / index
                    self.execute(
                        ChangeColumnTypes(
                            table.name,
                            schema,
                            index_str_max_columns,
                            sql_types,
                            cap_varchar_max=1024,
                        )
                    )
        if on_empty_table is None or not on_empty_table:
            self.add_table_primary_key(table, schema)
            self.add_table_indexes(table, schema)

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
                if last_use_statement is not None:
                    # ensure database connection is reset to original database
                    self.execute(f"USE {self.engine_url.database}", conn=conn)

    def __execute_raw_sql_pytsql(self, raw_sql: RawSql):
        """Use pytsql for executing T-SQL scripts containing many GO statements."""
        # noinspection PyUnresolvedReferences,PyPackageRequirements
        import pytsql

        sql_string = raw_sql.sql
        if self.print_sql:
            print_query_string = self.format_sql_string(sql_string)
            if len(print_query_string) >= self.max_query_print_length:
                print_query_string = (
                    print_query_string[: self.max_query_print_length] + " [...]"
                )
            self.logger.info("Executing sql", query=print_query_string)
        # ensure database connection is reset to original database
        sql_string += f"\nUSE {self.engine_url.database}"

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

    def resolve_alias(self, table: Table, stage_name: str):
        # The base implementation already takes care of converting Table objects
        # based on ExternalTableReference objects to string table name and schema.
        # For normal Table objects, it needs the stage schema name.
        table_name, schema = super().resolve_alias(table, stage_name)
        return PipedagMSSqlReflection.resolve_alias(self.engine, table_name, schema)


@MSSqlTableStore.register_table(pd)
class PandasTableHook(PandasTableHook):
    @classmethod
    def _get_dialect_dtypes(cls, dtypes: dict[str, Dtype], table: Table[pd.DataFrame]):
        _ = table
        return ({name: dtype.to_sql() for name, dtype in dtypes.items()}) | (
            {
                name: sa.dialects.mssql.DATETIME2()
                for name, dtype in dtypes.items()
                if dtype == Datetime()
            }
        )


try:
    import polars
except ImportError:
    polars = None


def reflect_pyodbc_column_types(query: str, odbc_string: str):
    """Reflect the column types of a query using pyodbc."""
    import pyodbc

    with pyodbc.connect(odbc_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT TOP 0 * FROM ({query}) as A")
            column_types = [
                {
                    "name": d[0],  # column name
                    "sql_type": d[1],  # ODBC SQL type code, e.g. -5 for BIGINT
                    "precision": d[4],  # column_size
                    "scale": d[5],  # decimal_digits
                    "nullable": d[6],  # 1 = nullable, 0 = NOT NULL
                }
                for d in cursor.description
            ]
    return column_types


@MSSqlTableStore.register_table(polars)
class PolarsTableHook(PolarsTableHook):
    @classmethod
    def _adjust_cols_retrieve(
        cls, cols: dict, dtypes: dict
    ) -> tuple[dict | None, dict]:
        arrow_odbc = True
        max_string_length = 256
        if arrow_odbc:
            # arrow-odbc does not handle VARCHAR(MAX) columns well, so we need to cut
            # strings
            if any(isinstance(col.type, sa.String) for col in cols.values()):
                return {
                    name: sa.cast(
                        sa.func.substring(col, 1, max_string_length).label(name),
                        sa.String(max_string_length),
                    )
                    if isinstance(col.type, sa.String)
                    else col
                    for name, col in cols.items()
                }, dtypes
        return None, dtypes

    @classmethod
    def _read_db_query(cls, store: SQLTableStore, table: Table, stage_name: str | None):
        table_name, schema = store.resolve_alias(table, stage_name)
        arrow_odbc = True

        if arrow_odbc:
            sql_table = store.reflect_table(table_name, schema).alias("tbl")

            cols = {col.name: col for col in sql_table.columns}
            dtypes = {name: Dtype.from_sql(col.type) for name, col in cols.items()}

            cols, dtypes = cls._adjust_cols_retrieve(cols, dtypes)
            if cols is not None:
                return sa.select(*cols.values()).select_from(sql_table)
        else:
            sql_table = sa.table(table_name, schema=schema)
        query = sa.select("*").select_from(sql_table)

        return query

    @classmethod
    def download_table(
        cls, query: str, connection_uri: str, store: SQLTableStore
    ) -> polars.DataFrame:
        engine = store.engine
        arrow_odbc = True

        if arrow_odbc:
            try:
                odbc_string = ConnectionString.from_url(engine.url).odbc_string

                # we need to avoid VARCHAR(MAX) columns since arrow-odbc does not
                # handle them well
                # dtypes = {}  # TODO: allow user to provide custom dtypes
                # column_types = reflect_pyodbc_column_types(query, odbc_string)

                import polars as pl

                df = pl.concat(
                    pl.read_database(
                        query=query,
                        connection=odbc_string,
                        iter_batches=True,
                        batch_size=65536,
                        # schema_overrides=dtypes,
                        execute_options={"map_schema": map_pyarrow_schema_for_polars},
                    ),
                    rechunk=True,
                )

                return df
            except Exception as e:  # noqa
                pass

        return super().download_table(query, connection_uri, store)


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


try:
    import pyarrow as pa
except ImportError:
    pa = types.ModuleType("pyarrow")
    pa.Schema = None


def map_pyarrow_schema_for_polars(schema: pa.Schema) -> pa.Schema:
    """This is necessary since, by default, arrow-odbc loads all DATETIME2 columns with
    nanosecond precision before potentially casting them to microsecond precision (as
    specified in the polars schema), which results in overflows.

    Hence, we have to tell arrow-odbc to load the fields only in microsecond precision.
    """
    assert pa.schema is not None, "please install pyarrow"
    mapped_fields = [
        (
            field.with_type(pa.timestamp("us"))
            if field.type == pa.timestamp("ns")
            else field
        )
        for field in list(schema)
    ]
    return pa.schema(mapped_fields, schema.metadata)


DEFAULT_PORT = 1433
MSSQL_DRIVER_REGEX = re.compile(r"ODBC Driver ([0-9]+) for SQL Server")
ODBC_CONNECTION_FIELD_KEYMAP = {
    "server": "host",
    "pwd": "password",
    "uid": "username",
    "trustservercertificate": "insecure",
}
TRUTH_STR_VALUES = ("y", "yes", "t", "true", "True", "on", "1", "1.0")
FALSE_STR_VALUES = ("n", "no", "f", "false", "False", "off", "0", "0.0")

TRUTH_VALUES = (*TRUTH_STR_VALUES, True, 1.0)
FALSE_VALUES = (*FALSE_STR_VALUES, False, 0.0)

NA_VALUES: list[Any] = ["nan", "NaN", "NaT", "NULL", "null", float("nan")]


def _latest_mssql_driver() -> str:
    import pyodbc

    # Get all drivers
    drivers = pyodbc.drivers()

    # Filter by SQL drivers and get their versions
    mssql_driver_versions = []
    for driver in drivers:
        match = MSSQL_DRIVER_REGEX.match(driver)
        if match:
            mssql_driver_versions.append(int(match.groups()[0]))
    if len(mssql_driver_versions) == 0:
        raise ValueError("No ODBC driver for SQL Server found.")

    # Return driver name of latest version
    latest_version = max(mssql_driver_versions)
    return f"ODBC Driver {latest_version} for SQL Server"


@dataclass
class ConnectionString:
    """Utility class for handling parameters of a database connection string.

    You typically initialize this connection string manually by passing values for at
    least ``host`` and ``database``. Afterwards, you will want to either...

    - create a :class:`~sqlalchemy.Engine` by using :meth:`sqlalchemy_url` or
    - create a connection with e.g. :mod:`pyodbc` by using ``str(self)``

    Note:
        You often have to either disable encryption or allow insecure connections
        within corporate networks.
    """

    #: The host of the database instance. Might be a hostname or an IP address.
    host: str
    #: The name of the database to connect to in the database instance.
    database: str = "tempdb"
    #: The port to connect to.
    port: int | None = DEFAULT_PORT
    #: The name of the ODBC driver to use for connecting to the database.
    driver: str = field(default_factory=_latest_mssql_driver)

    #: The username for connecting to the database (if not using Windows Auth).
    username: str | None = None
    #: The password of the user to authenticate with (if not using Windows Auth).
    password: str | None = None

    #: Whether to encrypt communication with the database in exchange for performance.
    encrypt: bool = True
    #: Whether to skip server certificate validation for TLS-encrypted communication.
    insecure: bool = False

    @classmethod
    def from_engine(cls, engine: Engine) -> "ConnectionString":
        return ConnectionString.from_url(engine.url)

    @classmethod
    def from_url(cls, url: URL) -> "ConnectionString":
        """Parse a ConnectionString from an url.

        Args:
            url: URL to parse; both sqlalchemy and odbc url formats are valid

        Returns:
            ConnectionString parsed from url
        """
        # url is a sqlalchemy url
        query = {k.lower(): v for k, v in url.query.items()}

        kwargs = {}
        if query.get("driver") is not None:
            kwargs["driver"] = query["driver"]
        if query.get("encrypt") is not None:
            kwargs["encrypt"] = (
                query["encrypt"].lower() not in FALSE_STR_VALUES  # type: ignore
            )
        if query.get("trustservercertificate") is not None:
            kwargs["insecure"] = (
                query["trustservercertificate"].lower()  # type: ignore
                in TRUTH_STR_VALUES
            )

        return ConnectionString(
            host=url.host or "localhost",
            database=url.database or "tempdb",
            port=url.port,
            username=url.username,
            password=url.password if url.username is not None else None,
            **kwargs,  # type: ignore
        )

    @classmethod
    def _from_odbc_string(cls, odbc_string: str):
        """Parse a ConnectionString from the fields of an odbc dictionary string.

        Args:
            odbc_string: odbc connection dictionary string

        Returns:
            ConnectionString parsed from the odbc connection arguments
        """
        kwargs: dict[str, Any] = {}
        for entry in odbc_string.split(";"):
            key_val = entry.split("=")
            assert len(key_val) == 2
            adjusted_key = ODBC_CONNECTION_FIELD_KEYMAP.get(
                key_val[0].lower(), key_val[0].lower()
            )
            if adjusted_key == "trusted_connection":
                continue  # we can determine this key from username presence
            kwargs[adjusted_key] = key_val[1]

        if "driver" in kwargs:
            kwargs["driver"] = kwargs["driver"].replace("{", "").replace("}", "")
        kwargs["encrypt"] = kwargs.get("encrypt", "").lower() not in FALSE_STR_VALUES
        kwargs["insecure"] = kwargs.get("insecure", "").lower() in TRUTH_STR_VALUES

        return ConnectionString(
            **kwargs,
        )

    def with_database(self, /, database: str) -> "ConnectionString":
        """Obtain a copy of the connection string reference a different database.

        The returned connection string connects to the same database instance as the
        original connection string and uses the same connection parameters.

        Args:
            database: The name of the database that the connection string should
                reference.

        Returns:
            The new connection string.
        """
        return dataclasses.replace(self, database=database)

    @property
    def driver_version(self) -> int:
        """The version of the ODBC driver referenced by the connection string."""
        match = MSSQL_DRIVER_REGEX.match(self.driver)
        if match is None:
            raise ValueError(f"Cannot determine driver version from '{self.driver}'.")
        return int(match.groups()[0])

    @property
    def odbc_string(self) -> str:
        """The ODBC connection string to use for :mod:`pyodbc` and :mod:`arrow-odbc`."""
        params = {
            "Driver": "{" + self.driver + "}",
            "Server": (f"{self.host},{self.port}" if self.port else self.host),
            "Database": self.database,
            "Encrypt": "yes" if self.encrypt else "no",
            "TrustServerCertificate": "yes" if self.insecure else "no",
        }
        if self.username is None:
            params["Trusted_Connection"] = "yes"
        else:
            params["UID"] = self.username
            if self.password is not None:
                params["PWD"] = self.password
        return ";".join(f"{k}={v}" for k, v in params.items())

    def fine_grained_odbc_string(
        self, multiple_active_result_sets: bool = False
    ) -> str:
        """Construct an ODBC connection string with more more fine-grained control."""
        return (
            self.odbc_string
            + f";MARS_Connection={'yes' if multiple_active_result_sets else 'no'}"
        )

    @property
    def sqlalchemy_url(self) -> URL:
        """The connection string as a URL for SQLAlchemy.

        This URL should be used when calling :meth:`sqlalchemy.create_engine`.

        Note:
            This method intentionally does NOT create a URL with the format
            ``mssql+pyodbc:///?odbc_connect={odbc_args}`` since this does not allow for
            accessing the ``database`` property of an engine URL in SQLAlchemy.
        """
        query = {
            "driver": self.driver,
            "Encrypt": "yes" if self.encrypt else "no",
            "TrustServerCertificate": "yes" if self.insecure else "no",
        }
        if self.username is None:
            query["Trusted_Authentication"] = "yes"
        return URL.create(
            drivername="mssql+pyodbc",
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query=query,
        )
