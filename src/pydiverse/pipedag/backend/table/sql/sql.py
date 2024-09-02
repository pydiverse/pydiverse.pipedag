from __future__ import annotations

import json
import textwrap
import time
import warnings
from collections.abc import Iterable
from contextlib import contextmanager
from typing import Any, Literal

import sqlalchemy as sa
import sqlalchemy.exc

from pydiverse.pipedag import Stage, Table
from pydiverse.pipedag.backend.table.base import BaseTableStore
from pydiverse.pipedag.backend.table.sql.ddl import (
    AddIndex,
    AddPrimaryKey,
    ChangeColumnNullable,
    CopyTable,
    CreateAlias,
    CreateDatabase,
    CreateSchema,
    DropAlias,
    DropSchema,
    DropSchemaContent,
    DropTable,
    RenameSchema,
    RenameTable,
    split_ddl_statement,
)
from pydiverse.pipedag.container import RawSql, Schema
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.context.context import (
    CacheValidationMode,
    ConfigContext,
    StageCommitTechnique,
)
from pydiverse.pipedag.context.run_context import DeferredTableStoreOp
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.core import MaterializingTask
from pydiverse.pipedag.materialize.metadata import (
    LazyTableMetadata,
    RawSqlMetadata,
    TaskMetadata,
)
from pydiverse.pipedag.util.hashing import stable_hash

DISABLE_DIALECT_REGISTRATION = "__DISABLE_DIALECT_REGISTRATION"


class SQLTableStore(BaseTableStore):
    """Table store that materializes tables to a SQL database

    Uses schema swapping for transactions:
    Creates a schema for each stage and a temporary schema for each
    transaction. If all tasks inside a stage succeed, swaps the schemas by
    renaming them.

    The correct dialect specific subclass of ``SQLTableStore`` gets initialized when
    based on the dialect found in the provided engine url during initialization.

    .. rubric:: Supported Tables

    The `SQLTableStore` can materialize (that is, store task output)
    and dematerialize (that is, retrieve task input) the following Table types:

    .. list-table::
       :widths: 30, 30, 30
       :header-rows: 1
       :stub-columns: 1

       * - Framework
         - Materialization
         - Dematerialization

       * - SQLAlchemy
         - | :py:class:`sa.sql.expression.Selectable
                        <sqlalchemy.sql.expression.Selectable>`
           | :py:class:`sa.sql.expression.TextClause
                        <sqlalchemy.sql.expression.TextClause>`
         - :py:class:`sa.Table <sqlalchemy.schema.Table>`

       * - Pandas
         - :py:class:`pd.DataFrame <pandas.DataFrame>`
         - :py:class:`pd.DataFrame <pandas.DataFrame>`

       * - Polars
         - | :external+pl:doc:`pl.DataFrame <reference/dataframe/index>`
           | :external+pl:doc:`pl.LazyFrame <reference/lazyframe/index>`
         - | :external+pl:doc:`pl.DataFrame <reference/dataframe/index>`
           | :external+pl:doc:`pl.LazyFrame <reference/lazyframe/index>`

       * - tidypolars
         - :py:class:`tp.Tibble <tidypolars.tibble.Tibble>`
         - :py:class:`tp.Tibble <tidypolars.tibble.Tibble>`

       * - Ibis
         - :py:class:`ibis.api.Table <ibis.expr.types.relations.Table>`
         - :py:class:`ibis.api.Table <ibis.expr.types.relations.Table>`

       * - pydiverse.transform
         - ``pdt.Table``
         - | ``pdt.eager.PandasTableImpl``
           | ``pdt.lazy.SQLTableImpl``

       * - pydiverse.pipedag table reference
         - :py:class:`~.ExternalTableReference` (no materialization)
         - Can be read with all dematerialization methods above

    :param url:
        The :external+sa:ref:`SQLAlchemy engine url <database_urls>`
        used to connect to the database.

        This URL may contain placeholders like ``{name}`` or ``{instance_id}``
        (additional ones can be defined in the ``url_attrs_file``) or
        environment variables like ``{$USER}`` which get substituted with their
        respective values.

        Attention: passwords including special characters like ``@`` or ``:`` need
        to be URL encoded.

    :param url_attrs_file:
        Filename of a yaml file which is read shortly before rendering the final
        engine URL and which is used to replace custom placeholders in ``url``.

        Just like ``url``, this value may also contain placeholders and environment
        variables which get substituted.

    :param create_database_if_not_exists:
        If the engine url references a database name that doesn't yet exists,
        then setting this value to ``True`` tells pipedag to create the database
        before trying to open a connection to it.

    :param schema_prefix:
        A prefix that gets placed in front of all schema names created by pipedag.

    :param schema_suffix:
        A suffix that gets placed behind of all schema names created by pipedag.

    :param avoid_drop_create_schema:
        If ``True``, no ``CREATE SCHEMA`` or ``DROP SCHEMA`` statements get issued.
        This is mostly relevant for databases that support automatic schema
        creation like IBM DB2.

    :param print_materialize:
        If ``True``, all tables that get materialized get logged.

    :param print_sql:
        If ``True``, all executed SQL statements get logged.

    :param no_db_locking:
        Speed up database by telling it we will not rely on it's locking mechanisms.
        Currently not implemented.

    :param strict_materialization_details:
        If ``True``: raise an exception if
            - the argument ``materialization_details`` is given even though the
              table store does not support it.
            - a table references a ``materialization_details`` tag that is not defined
              in the config.
        If ``False``: Log an error instead of raising an exception

    :param materialization_details:
        A dictionary with each entry describing a tag for materialization details of
        the table store. See subclasses of :py:class:`BaseMaterializationDetails
         <pydiverse.pipedag.materialize.details.BaseMaterializationDetails>`
        for details.
    :param default_materialization_details:
        The materialization_details that will be used if materialization_details
        is not specified on table level. If not set, the ``__any__`` tag (if specified)
        will be used.
    :param max_concurrent_copy_operations:
        In case of a partially cache-valid stage, we need to copy tables from the
        cache schema to the new transaction schema. This parameter specifies the
        maximum number of workers we use for concurrently copying tables.
    :param sqlalchemy_pool_size:
        The number of connections to keep open inside the connection pool.
        It is recommended to choose a larger number than
        ``max_concurrent_copy_operations`` to avoid running into pool_timeout.
    :param sqlalchemy_pool_timeout:
        The number of seconds to wait before giving up on getting a connection from
        the pool. This may be relevant in case the connection pool is saturated
        with concurrent operations each working with one or more database connections.
    """

    METADATA_SCHEMA = "pipedag_metadata"
    LOCK_SCHEMA = "pipedag_locks"

    __registered_dialects: dict[str, type[SQLTableStore]] = {}
    _dialect_name: str

    def __new__(cls, engine_url: str, *args, **kwargs):
        if cls != SQLTableStore:
            return super().__new__(cls)

        # If calling SQLTableStore(engine_url), then we want to dynamically instantiate
        # the correct dialect specific subclass based on the dialect found in the url.
        dialect = sa.engine.make_url(engine_url).get_dialect().name
        dialect_specific_cls = SQLTableStore.__registered_dialects.get(dialect, cls)
        return super(SQLTableStore, dialect_specific_cls).__new__(dialect_specific_cls)

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        config = config.copy()
        engine_url = config.pop("url")
        return cls(engine_url, **config)

    def __init__(
        self,
        engine_url: str,
        *,
        create_database_if_not_exists: bool = False,
        schema_prefix: str = "",
        schema_suffix: str = "",
        avoid_drop_create_schema: bool = False,
        print_materialize: bool = False,
        print_sql: bool = False,
        no_db_locking: bool = True,
        strict_materialization_details: bool = True,
        materialization_details: dict[str, dict[str | list[str]]] | None = None,
        default_materialization_details: str | None = None,
        max_concurrent_copy_operations: int = 5,
        sqlalchemy_pool_size: int = 12,
        sqlalchemy_pool_timeout: int = 300,
    ):
        super().__init__()

        self.create_database_if_not_exists = create_database_if_not_exists
        self.schema_prefix = schema_prefix
        self.schema_suffix = schema_suffix
        self.avoid_drop_create_schema = avoid_drop_create_schema
        self.print_materialize = print_materialize
        self.print_sql = print_sql
        self.no_db_locking = no_db_locking
        self.strict_materialization_details = strict_materialization_details
        self.max_concurrent_copy_operations = max_concurrent_copy_operations
        self.sqlalchemy_pool_size = sqlalchemy_pool_size
        self.squalchemy_pool_timeout = sqlalchemy_pool_timeout

        self.metadata_schema = self.get_schema(self.METADATA_SCHEMA)
        self.engine_url = sa.engine.make_url(engine_url)
        self.engine = self._create_engine()

        # Dict where hooks are allowed to store values that should get cached
        self.hook_cache = {}

        self._init_database()

        # Set up metadata tables and schema
        self.sql_metadata = sa.MetaData(schema=self.metadata_schema.get())

        # Store version number for metadata table schema evolution.
        # We disable caching in case of version mismatch.
        self.disable_caching = False
        self.metadata_version = "0.3.2"  # Increase version if metadata table changes
        self.version_table = sa.Table(
            "metadata_version",
            self.sql_metadata,
            sa.Column("version", sa.String(32)),
        )

        # Stage Table is unique for stage
        self.stage_table = sa.Table(
            "stages",
            self.sql_metadata,
            self._metadata_pk("id", "stages"),
            sa.Column("stage", sa.String(64)),
            sa.Column("cur_transaction_name", sa.String(256)),
        )

        # Task Table is unique for stage * in_transaction_schema
        self.tasks_table = sa.Table(
            "tasks",
            self.sql_metadata,
            self._metadata_pk("id", "tasks"),
            sa.Column("name", sa.String(128)),
            sa.Column("stage", sa.String(64)),
            sa.Column("version", sa.String(64)),
            sa.Column("timestamp", sa.DateTime()),
            sa.Column("run_id", sa.String(20)),
            sa.Column("position_hash", sa.String(20)),
            sa.Column("input_hash", sa.String(20)),
            sa.Column("cache_fn_hash", sa.String(20)),
            sa.Column("output_json", sa.Text()),
            sa.Column("in_transaction_schema", sa.Boolean()),
        )

        # Lazy Cache Table is unique for stage * in_transaction_schema
        self.lazy_cache_table = sa.Table(
            "lazy_tables",
            self.sql_metadata,
            self._metadata_pk("id", "lazy_tables"),
            sa.Column("name", sa.String(128)),
            sa.Column("stage", sa.String(64)),
            sa.Column("query_hash", sa.String(20)),
            sa.Column("task_hash", sa.String(20)),
            sa.Column("in_transaction_schema", sa.Boolean()),
        )

        # Sql Cache Table is unique for stage * in_transaction_schema
        self.raw_sql_cache_table = sa.Table(
            "raw_sql",
            self.sql_metadata,
            self._metadata_pk("id", "raw_sql"),
            sa.Column("prev_objects", sa.Text()),
            sa.Column("new_objects", sa.Text()),
            sa.Column("stage", sa.String(64)),
            sa.Column("query_hash", sa.String(20)),
            sa.Column("task_hash", sa.String(20)),
            sa.Column("in_transaction_schema", sa.Boolean()),
        )

        self.default_materialization_details = default_materialization_details

        self._set_materialization_details(materialization_details)

        self.logger.info(
            "Initialized SQL Table Store",
            engine_url=self.engine_url.render_as_string(hide_password=True),
            schema_prefix=self.schema_prefix,
            schema_suffix=self.schema_suffix,
        )

    def __init_subclass__(cls, **kwargs):
        # Whenever a new subclass if SQLTableStore is defined, it must contain the
        # `_dialect_name` attribute. This allows us to dynamically instantiate it
        # when calling SQLTableStore(engine_url, ...) based on the dialect name found
        # in the engine url (see __new__).
        dialect_name = getattr(cls, "_dialect_name", None)
        if dialect_name is None:
            raise ValueError(
                "All subclasses of SQLTableStore must have a `_dialect_name` attribute."
                f" But {cls.__name__}._dialect_name is None."
            )

        if dialect_name != DISABLE_DIALECT_REGISTRATION:
            if dialect_name in SQLTableStore.__registered_dialects:
                warnings.warn(
                    f"Already registered a SQLTableStore for dialect {dialect_name}"
                )
            SQLTableStore.__registered_dialects[dialect_name] = cls

    def _metadata_pk(self, name: str, table_name: str):
        return sa.Column(name, sa.BigInteger(), primary_key=True)

    def _init_database(self):
        if not self.create_database_if_not_exists:
            return

        raise NotImplementedError(
            "create_database_if_not_exists is not implemented for this engine"
        )

    def _init_database_with_database(
        self,
        database: str,
        execution_options: dict = None,
        create_database: str | None = None,
        disable_exists_check=False,
    ):
        if not self.create_database_if_not_exists:
            return

        if execution_options is None:
            execution_options = {}

        if not disable_exists_check:
            try:
                # Check if database exists
                with self.engine.connect() as conn:
                    conn.exec_driver_sql("SELECT 1")
                return
            except sa.exc.DBAPIError:
                # Database doesn't exist
                pass

        # Create new database using temporary engine object
        tmp_db_url = self.engine_url.set(database=database)
        tmp_engine = sa.create_engine(tmp_db_url, execution_options=execution_options)

        try:
            if create_database is None:
                create_database = self.engine_url.database
            with tmp_engine.connect() as conn:
                conn.execute(CreateDatabase(create_database))
        except sa.exc.DBAPIError:
            # This happens if multiple instances try to create the database
            # at the same time.
            with self.engine.connect() as conn:
                # Verify database actually exists
                conn.exec_driver_sql("SELECT 1")

    def _default_isolation_level(self) -> str | None:
        # we never want database transactional behavior (concurrent read+write)
        return "READ UNCOMMITTED"

    def _create_engine(self):
        kwargs = {}
        if self._default_isolation_level() is not None:
            kwargs["isolation_level"] = self._default_isolation_level()

        # future=True enables SQLAlchemy 2.0 behaviour with version 1.4
        # However, it does not support pd.read_sql_table
        return sa.create_engine(
            self.engine_url,
            # future=True,
            pool_size=self.sqlalchemy_pool_size,
            pool_timeout=self.squalchemy_pool_timeout,
            **kwargs,
        )

    @contextmanager
    def engine_connect(self) -> sa.Connection:
        with self.engine.connect() as conn:
            yield conn
            if sa.__version__ >= "2.0.0":
                conn.commit()

    def get_schema(self, name: str) -> Schema:
        return Schema(name, self.schema_prefix, self.schema_suffix)

    def _execute(
        self, query, conn: sa.engine.Connection, truncate_printed_select=False
    ):
        if self.print_sql:
            if isinstance(query, str):
                query_str = query
            else:
                query_str = str(
                    query.compile(self.engine, compile_kwargs={"literal_binds": True})
                )
            pretty_query_str = self.format_sql_string(
                query_str, truncate_printed_select
            )
            self.logger.info("Executing sql", query=pretty_query_str)

        if isinstance(query, str):
            ret = conn.execute(sa.text(query))
        else:
            ret = conn.execute(query)
        return ret

    def execute(
        self,
        query: str
        | sqlalchemy.sql.expression.Selectable
        | sqlalchemy.sql.expression.TextClause
        | sqlalchemy.sql.ddl.DDLElement
        | list[
            str
            | sqlalchemy.sql.expression.Selectable
            | sqlalchemy.sql.expression.TextClause
            | sqlalchemy.sql.ddl.DDLElement
        ],
        *,
        conn: sa.engine.Connection = None,
        truncate_printed_select=False,
    ):
        if conn is None:
            with self.engine_connect() as conn:
                return self.execute(
                    query,
                    conn=conn,
                    truncate_printed_select=truncate_printed_select,
                )

        if isinstance(query, sa.schema.DDLElement) or isinstance(query, list):
            if not isinstance(query, list):
                query = [query]

            @contextmanager
            def transactional():
                if conn.in_transaction():
                    yield
                else:
                    with conn.begin():
                        yield

            with transactional():
                # execute multiple statements in one transaction
                for cur_query in query:
                    if isinstance(cur_query, sa.schema.DDLElement):
                        # Some custom DDL statements contain multiple statements.
                        # They are all seperated using a special seperator.
                        query_str = str(
                            cur_query.compile(
                                self.engine, compile_kwargs={"literal_binds": True}
                            )
                        )

                        for part in split_ddl_statement(query_str):
                            self._execute(part, conn, truncate_printed_select)
                    else:
                        self._execute(cur_query, conn, truncate_printed_select)
            return

        return self._execute(query, conn, truncate_printed_select)

    def reflect_table(self, table_name: str, schema: str | Schema) -> sa.Table:
        if isinstance(schema, Schema):
            schema = schema.get()
        tbl = None
        for retry_iteration in range(4):
            # retry operation since it might have been terminated as a deadlock victim
            try:
                tbl = sa.Table(
                    table_name,
                    sa.MetaData(),
                    schema=schema,
                    autoload_with=self.engine,
                )
                break
            except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError):
                if retry_iteration == 3:
                    raise
                time.sleep(retry_iteration * retry_iteration * 1.2)
        return tbl

    def check_materialization_details_supported(self, label: str | None) -> None:
        if label is None:
            return
        error_msg = (
            f"Materialization details are not supported"
            f" for store {type(self).__name__}."
        )
        if self.strict_materialization_details:
            raise ValueError(
                f"{error_msg} To silence this exception set"
                f" strict_materialization_details=False"
            )
        else:
            self.logger.error(f"{error_msg}")

    def _set_materialization_details(
        self, materialization_details: dict[str, dict[str | list[str]]] | None
    ) -> None:
        if (
            materialization_details is not None
            or self.default_materialization_details is not None
        ):
            error_msg = (
                f"{type(self).__name__} does not support materialization details."
            )
            if self.strict_materialization_details:
                raise TypeError(
                    f"{error_msg} To suppress this exception, "
                    f"use strict_materialization_details=False"
                )
            self.logger.error(error_msg)

    def get_unlogged(self, materialization_details_label: str | None):
        _ = materialization_details_label
        return False  # only available for some dialects

    def get_create_table_suffix(self, materialization_details_label: str | None) -> str:
        """
        This method can be used to append materialization tables to CREATE TABLE
        statements by dialects that support it.

        :param materialization_details_label:
            A label that can be controlled on table or stage level to reference
            options layed out in detail in configuration
        :return:
            Suffix that is appended to CREATE TABLE statement as string
        """
        return ""

    def get_limit_query(
        self,
        query: sa.sql.expression.Selectable | sa.sql.expression.TextClause,
        rows: int,
    ) -> sa.sql.expression.Select:
        if isinstance(query, sa.sql.expression.TextClause):
            return (
                sa.select(sa.text("*"))
                .limit(rows)
                .select_from(sa.text("(" + str(query) + ") AS A"))
            )
        else:
            return sa.select(sa.text("*")).limit(rows).select_from(query.alias("A"))

    def lock_table(
        self, table: Table | str, schema: Schema | str, conn: Any = None
    ) -> list:
        """
        For some dialects, it might be beneficial to lock a table before writing to it.

        :returns
            A list of SQLAlchemy statements that should be executed if conn=None
        """
        _ = table, schema, conn
        return []

    def lock_source_table(
        self, table: Table | str, schema: Schema | str, conn: Any = None
    ) -> list:
        """
        For some dialects, it might be beneficial to lock source tables before reading.

        :returns
            A list of SQLAlchemy statements that should be executed if conn=None
        """
        _ = table, schema, conn
        return []

    def lock_source_tables(
        self, tables: list[dict[str, str]], conn: Any = None
    ) -> list:
        """
        For some dialects, it might be beneficial to lock source tables before reading.

        :returns
            A list of SQLAlchemy statements that should be executed if conn=None
        """
        stmts = []
        for table in tables:
            if table["shared_lock_allowed"]:
                stmts += self.lock_source_table(table["name"], table["schema"], conn)
        return stmts

    def get_forced_nullability_columns(
        self, table: Table, table_cols: Iterable[str]
    ) -> tuple[list[str], list[str]]:
        """
        Get the columns that should be forced to be nullable or non-nullable.

        This is typically applied on the materialized empty table, but may also
        be applied after filling the table by some dialects. Some dialects may
        choose to add primary key columns to non_nullable_cols if they need it
        to set the primary key. However, some dialects do this separately because
        they also need to change the type of primary key columns. Some dialects
        only return non-nullable columns because all columns will be nullable
        after initial materialization.

        :param table:
            Table to be materialized. It includes attributes like nullable,
            non_nullable, and primary_key which may influence the return value.
        :param table_cols:
            The list of all columns in the table. If nullable and non_nullable is
            specified, all columns must be mentioned. If only one of them is set,
            the other one is calculated as the remaining set.
        :return:
            A tuple of two lists. The first list contains the columns that should
            be forced to be nullable, the second list contains the columns that
            should be forced to be non-nullable. Most dialects will not need to
            force nullable columns because very column is nullable by default.
        """
        _, non_nullable_cols = self._process_table_nullable_parameters(
            table, table_cols
        )

        # in most dialects columns are nullable by default
        return [], non_nullable_cols

    @staticmethod
    def _process_table_nullable_parameters(table: Table, table_cols: Iterable[str]):
        name = f'"{table.name}"'
        nullable_set = set(table.nullable or [])
        non_nullable_set = set(table.non_nullable or [])
        table_cols_set = set(table_cols)

        for prefix, col_set, cols in [
            ("", nullable_set, table.nullable),
            ("non_", non_nullable_set, table.non_nullable),
        ]:
            if invalid_cols := col_set - table_cols_set:
                raise ValueError(
                    f"The columns {invalid_cols} in Table({name},"
                    f" {prefix}nullable={cols}) aren't contained in the table"
                    f" columns: {table_cols}"
                )

        if table.nullable is not None and table.non_nullable is not None:
            # If both are specified, they aren't allowed to intersect and
            # their union must be the set of all columns.
            if intersection := nullable_set & non_nullable_set:
                raise ValueError(
                    f"The columns {intersection} are both set to be nullable and non"
                    f" nullable in Table({name}, nullable={table.nullable},"
                    f" non_nullable={table.non_nullable})"
                )
            if missing_cols := table_cols_set - nullable_set - non_nullable_set:
                raise ValueError(
                    f"When setting Table({name}, nullable={table.nullable},"
                    f" non_nullable={table.non_nullable}), all columns of the"
                    f" table must be mentioned. Missing columns: {missing_cols}"
                )
            nullable = table.nullable
            non_nullable = table.non_nullable
        elif table.nullable is not None:
            non_nullable = [c for c in table_cols if c not in nullable_set]
            nullable = table.nullable
        elif table.non_nullable is not None:
            nullable = [c for c in table_cols if c not in non_nullable_set]
            non_nullable = table.non_nullable
        else:
            nullable = []
            non_nullable = []

        return nullable, non_nullable

    def dialect_requests_empty_creation(self, table: Table, is_sql: bool) -> bool:
        _ = is_sql
        return table.nullable is not None or table.non_nullable is not None

    def add_indexes_and_set_nullable(
        self,
        table: Table,
        schema: Schema,
        *,
        on_empty_table: bool | None = None,
        table_cols: Iterable[str] | None = None,
    ):
        if on_empty_table is None or on_empty_table:
            # By default, we set non-nullable on empty table
            if table_cols is None:
                # reflect table:
                inspector = sa.inspect(self.engine)
                columns = inspector.get_columns(table.name, schema=schema.get())
                table_cols = [col["name"] for col in columns]
            nullable_cols, non_nullable_cols = self.get_forced_nullability_columns(
                table, table_cols
            )
            if len(nullable_cols) > 0:
                # some dialects represent literals as non-nullable types
                self.execute(
                    ChangeColumnNullable(
                        table.name, schema, nullable_cols, nullable=True
                    )
                )
            if len(non_nullable_cols) > 0:
                self.execute(
                    ChangeColumnNullable(
                        table.name, schema, non_nullable_cols, nullable=False
                    )
                )
        if on_empty_table is None or not on_empty_table:
            # By default, we set indexes after loading full table. This can be
            # overridden by dialect
            self.add_table_primary_key(table, schema)
            self.add_table_indexes(table, schema)

    def add_table_indexes(self, table: Table, schema: Schema):
        if table.indexes is not None:
            for index in table.indexes:
                self.add_index(table.name, schema, index)

    def add_table_primary_key(self, table: Table, schema: Schema):
        if table.primary_key is not None:
            key = table.primary_key
            if isinstance(key, str):
                key = [key]
            self.add_primary_key(table.name, schema, key)

    def add_primary_key(
        self,
        table_name: str,
        schema: Schema,
        key_columns: list[str],
        *,
        name: str | None = None,
    ):
        self.execute(AddPrimaryKey(table_name, schema, key_columns, name))

    def add_index(
        self,
        table_name: str,
        schema: Schema,
        index_columns: list[str],
        name: str | None = None,
    ):
        self.execute(AddIndex(table_name, schema, index_columns, name))

    def copy_indexes(
        self, src_table: str, src_schema: Schema, dest_table: str, dest_schema: Schema
    ):
        inspector = sa.inspect(self.engine)
        pk_constraint = inspector.get_pk_constraint(src_table, schema=src_schema.get())
        if len(pk_constraint["constrained_columns"]) > 0:
            self.add_primary_key(
                dest_table,
                dest_schema,
                pk_constraint["constrained_columns"],
                name=pk_constraint["name"],
            )
        indexes = inspector.get_indexes(src_table, schema=src_schema.get())
        for index in indexes:
            if len(index["include_columns"]) > 0:
                self.logger.warning(
                    "Perfect index recreation in caching is not net implemented",
                    table=dest_table,
                    schema=dest_schema.get(),
                    include_columns=index["include_columns"],
                )
            if any(
                not isinstance(val, list) or len(val) > 0
                for val in index["dialect_options"].values()
            ):
                self.logger.warning(
                    "Perfect index recreation in caching is not net implemented",
                    table=dest_table,
                    schema=dest_schema.get(),
                    dialect_options=index["dialect_options"],
                )
            self.add_index(
                dest_table, dest_schema, index["column_names"], index["name"]
            )

    def reflect_sql_types(
        self, col_names: Iterable[str], table_name: str, schema: Schema
    ):
        inspector = sa.inspect(self.engine)
        columns = inspector.get_columns(table_name, schema=schema.get())
        types = {d["name"]: d["type"] for d in columns}
        sql_types = [types[col] for col in col_names]
        return sql_types

    @staticmethod
    def format_sql_string(query_str: str, truncate_select=False) -> str:
        if truncate_select and "SELECT" in query_str:
            query_str = query_str.split("SELECT")[0] + "SELECT ...\n"
        return textwrap.dedent(query_str).strip()

    def execute_raw_sql(self, raw_sql: RawSql):
        """Executed raw SQL statements in the associated transaction stage"""
        for statement in raw_sql.sql.split(raw_sql.separator):
            if not statement.strip():
                # Skip empty queries
                continue

            self.execute(statement)

    def setup(self):
        super().setup()
        if not self.avoid_drop_create_schema:
            self.execute(CreateSchema(self.metadata_schema, if_not_exists=True))
        with self.engine_connect() as conn:
            try:
                version = conn.execute(
                    sa.select(self.version_table.c.version)
                ).scalar_one_or_none()
            except sa.exc.ProgrammingError:
                # table metadata_version does not yet exist
                version = None
        if version is None:
            with self.engine_connect() as conn, conn.begin():
                self.sql_metadata.create_all(conn)
                version = conn.execute(
                    sa.select(self.version_table.c.version)
                ).scalar_one_or_none()
                assert version is None  # detect race condition
                conn.execute(
                    self.version_table.insert().values(
                        version=self.metadata_version,
                    )
                )
        elif version != self.metadata_version:
            # disable caching due to incompatible metadata table schemas
            # (in future versions, consider automatic metadata schema upgrade)
            self.disable_caching = True
            self.logger.warning(
                "Disabled caching due to metadata version mismatch",
                version=version,
                expected_version=self.metadata_version,
            )

    def dispose(self):
        self.engine.dispose()
        self.hook_cache = None
        super().dispose()

    def init_stage(self, stage: Stage):
        stage_commit_technique = ConfigContext.get().stage_commit_technique
        if stage_commit_technique == StageCommitTechnique.SCHEMA_SWAP:
            return self._init_stage_schema_swap(stage)
        if stage_commit_technique == StageCommitTechnique.READ_VIEWS:
            return self._init_stage_read_views(stage)
        raise ValueError(f"Invalid stage commit technique: {stage_commit_technique}")

    def optional_pause_for_db_transactionality(
        self,
        prev_action: Literal[
            "table_drop",
            "table_create",
            "schema_drop",
            "schema_create",
            "schema_rename",
        ],
    ):
        _ = type
        # Some backends have transactionality problems with very quick
        # DROP/CREATE or RENAME activities for both schemas and tables
        # which happen in testing.
        pass

    def _init_stage_schema_swap(self, stage: Stage):
        schema = self.get_schema(stage.name)
        transaction_schema = self.get_schema(stage.transaction_name)

        cs_base = CreateSchema(schema, if_not_exists=True)
        ds_trans = DropSchema(
            transaction_schema, if_exists=True, cascade=True, engine=self.engine
        )
        cs_trans = CreateSchema(transaction_schema, if_not_exists=False)

        with self.engine_connect() as conn:
            if self.avoid_drop_create_schema:
                self.execute(
                    DropSchemaContent(transaction_schema, self.engine), conn=conn
                )
                self.optional_pause_for_db_transactionality("table_drop")
            else:
                self.execute(cs_base, conn=conn)
                self.execute(ds_trans, conn=conn)
                self.optional_pause_for_db_transactionality("schema_drop")
                self.execute(cs_trans, conn=conn)
                self.optional_pause_for_db_transactionality("schema_create")

            if not self.disable_caching:
                for table in [
                    self.tasks_table,
                    self.lazy_cache_table,
                    self.raw_sql_cache_table,
                ]:
                    conn.execute(
                        table.delete()
                        .where(table.c.stage == stage.name)
                        .where(table.c.in_transaction_schema)
                    )

    def _init_stage_read_views(self, stage: Stage):
        try:
            with self.engine_connect() as conn:
                metadata_rows = (
                    conn.execute(
                        self.stage_table.select().where(
                            self.stage_table.c.stage == stage.name
                        )
                    )
                    .mappings()
                    .one()
                )
            cur_transaction_name = metadata_rows["cur_transaction_name"]
        except (sa.exc.MultipleResultsFound, sa.exc.NoResultFound):
            cur_transaction_name = ""

        suffix = "__even" if cur_transaction_name.endswith("__odd") else "__odd"
        new_transaction_name = stage.name + suffix

        # TODO: WE SHOULD NOT MODIFY THE STAGE AFTER CREATION!!!
        stage.set_transaction_name(new_transaction_name)

        # Call schema swap function with updated transaction name
        self._init_stage_schema_swap(stage)

    def commit_stage(self, stage: Stage):
        # If the stage is 100% cache valid, then we just need to update the
        # "in_transaction_schema" column of the metadata tables.
        if not RunContext.get().has_stage_changed(stage):
            self.logger.info("Stage is cache valid", stage=stage)
            with self.engine_connect() as conn:
                self._commit_stage_update_metadata(stage, conn)
            return

        # Commit normally
        stage_commit_technique = ConfigContext.get().stage_commit_technique
        if stage_commit_technique == StageCommitTechnique.SCHEMA_SWAP:
            return self._commit_stage_schema_swap(stage)
        if stage_commit_technique == StageCommitTechnique.READ_VIEWS:
            return self._commit_stage_read_views(stage)
        raise ValueError(f"Invalid stage commit technique: {stage_commit_technique}")

    def _commit_stage_schema_swap(self, stage: Stage):
        schema = self.get_schema(stage.name)
        transaction_schema = self.get_schema(stage.transaction_name)
        tmp_schema = self.get_schema(stage.name + "__swap")

        # potentially this disposal must be optional since it does not allow
        # for multithreaded stage execution
        # dispose open connections which may prevent schema swapping
        self.engine.dispose()

        # We might want to also append ', conn.begin()' to the with statement,
        # but this collides with the inner conn.begin() used to execute
        # statement parts as one transaction. This is solvable via complex code,
        # but we typically don't want to rely on database transactions anyways.
        with self.engine_connect() as conn:
            # TODO: self.avoid_drop_create_schema is currently being ignored...
            self.execute(
                DropSchema(
                    tmp_schema, if_exists=True, cascade=True, engine=self.engine
                ),
                conn=conn,
            )
            self.optional_pause_for_db_transactionality("schema_drop")
            self.execute(RenameSchema(schema, tmp_schema, self.engine), conn=conn)
            self.optional_pause_for_db_transactionality("schema_rename")
            self.execute(
                RenameSchema(transaction_schema, schema, self.engine), conn=conn
            )
            self.optional_pause_for_db_transactionality("schema_rename")
            self.execute(
                RenameSchema(tmp_schema, transaction_schema, self.engine), conn=conn
            )
            self.optional_pause_for_db_transactionality("schema_rename")

            self._commit_stage_update_metadata(stage, conn=conn)

    def _commit_stage_read_views(self, stage: Stage):
        dest_schema = self.get_schema(stage.name)
        src_schema = self.get_schema(stage.transaction_name)

        with self.engine_connect() as conn:
            # Clear contents of destination schema
            self.execute(DropSchemaContent(dest_schema, self.engine), conn=conn)
            self.optional_pause_for_db_transactionality("table_drop")

            # Create aliases for all tables in transaction schema
            inspector = sa.inspect(self.engine)
            for table in inspector.get_table_names(schema=src_schema.get()):
                self.execute(CreateAlias(table, src_schema, table, dest_schema))

            # Update metadata
            stage_metadata_exists = (
                conn.execute(
                    sa.select(1).where(self.stage_table.c.stage == stage.name)
                ).scalar()
                == 1
            )

            if stage_metadata_exists:
                conn.execute(
                    self.stage_table.update()
                    .where(self.stage_table.c.stage == stage.name)
                    .values(cur_transaction_name=stage.transaction_name)
                )
            else:
                conn.execute(
                    self.stage_table.insert().values(
                        stage=stage.name,
                        cur_transaction_name=stage.transaction_name,
                    )
                )

            self._commit_stage_update_metadata(stage, conn=conn)

    def _commit_stage_update_metadata(self, stage: Stage, conn: sa.engine.Connection):
        if not self.disable_caching:
            for table in [
                self.tasks_table,
                self.lazy_cache_table,
                self.raw_sql_cache_table,
            ]:
                conn.execute(
                    table.delete()
                    .where(table.c.stage == stage.name)
                    .where(~table.c.in_transaction_schema)
                )
                conn.execute(
                    table.update()
                    .where(table.c.stage == stage.name)
                    .values(in_transaction_schema=False)
                )

    def copy_table_to_transaction(self, table: Table):
        from_schema = self.get_schema(table.stage.name)
        from_name = table.name

        if RunContext.get().has_stage_changed(table.stage):
            self._copy_table(table, from_schema, from_name)
        else:
            self._deferred_copy_table(table, from_schema, from_name)

    def copy_lazy_table_to_transaction(self, metadata: LazyTableMetadata, table: Table):
        from_schema = self.get_schema(metadata.stage)
        from_name = metadata.name

        if RunContext.get().has_stage_changed(table.stage):
            self._copy_table(table, from_schema, from_name)
        else:
            self._deferred_copy_table(table, from_schema, from_name)

    def _copy_table(self, table: Table, from_schema: Schema, from_name: str):
        """Copies the table immediately"""
        has_table = self.has_table_or_view(from_name, schema=from_schema)
        if not has_table:
            inspector = sa.inspect(self.engine)
            available_tables = inspector.get_table_names(
                from_schema.get()
            ) + inspector.get_view_names(from_schema.get())
            msg = (
                f"Can't copy table '{from_name}' (schema: '{from_schema}') to "
                f"transaction because no such table exists.\n"
                f"Tables in schema: {available_tables}"
            )
            self.logger.error(msg)
            raise CacheError(msg)

        from_tbl = sa.Table(from_name, sa.MetaData(), schema=from_schema.get())
        dest_tbl = table.copy_without_obj()
        dest_tbl.obj = from_tbl
        dest_tbl.shared_lock_allowed = True

        # Copy table via executing a select statement with respective hook
        RunContext.get().trace_hook.cache_pre_transfer(dest_tbl)
        hook = self.get_m_table_hook(type(dest_tbl.obj))
        hook.materialize(self, dest_tbl, dest_tbl.stage.transaction_name)
        RunContext.get().trace_hook.cache_post_transfer(dest_tbl)

    def _deferred_copy_table(
        self,
        table: Table,
        from_schema: Schema,
        from_name: str,
    ):
        """Tries to perform a deferred copy

        As long as the stage is determined to still be 100% cache valid, we don't
        actually copy any tables to the transaction, and instead just create an alias
        from the main schema to the transaction schema.

        If at the end of the stage, the all tables in the stage are cache valid, then
        we don't actually need to commit the stage, and instead just rename the
        tables from their old names to their new names.

        Otherwise, we copy the tables to the transaction schema in the background,
        and once we're ready to commit, we replace the aliases with the copied tables.
        """
        assert from_schema == self.get_schema(table.stage.name)

        has_table = self.has_table_or_view(from_name, from_schema)
        if not has_table:
            inspector = sa.inspect(self.engine)
            available_tables = inspector.get_table_names(
                from_schema.get()
            ) + inspector.get_view_names(from_schema.get())
            msg = (
                f"Can't deferred copy table '{from_name}' (schema: '{from_schema}') to "
                f"transaction because no such table exists.\n"
                f"Tables in schema: {available_tables}"
            )
            self.logger.error(msg)
            raise CacheError(msg)

        try:
            ctx = RunContext.get()
            stage = table.stage

            self.execute(
                CreateAlias(
                    from_name,
                    from_schema,
                    table.name,
                    self.get_schema(stage.transaction_name),
                )
            )

            table_copy = table.copy_without_obj()
            table_copy.name = f"{table.name}__copy"

            ctx.defer_table_store_op(
                stage,
                DeferredTableStoreOp(
                    "_copy_table",
                    DeferredTableStoreOp.Condition.ON_STAGE_CHANGED,
                    (
                        table_copy,
                        from_schema,
                        from_name,
                    ),
                ),
            )
            ctx.defer_table_store_op(
                stage,
                DeferredTableStoreOp(
                    "_swap_alias_with_table_copy",
                    DeferredTableStoreOp.Condition.ON_STAGE_COMMIT,
                    (),
                    {"table": table, "table_copy": table_copy},
                ),
            )
            ctx.defer_table_store_op(
                stage,
                DeferredTableStoreOp(
                    "_rename_table",
                    DeferredTableStoreOp.Condition.ON_STAGE_ABORT,
                    (from_name, table.name, from_schema),
                ),
            )

        except Exception as _e:
            msg = (
                f"Failed to copy table {from_name} (schema: '{from_schema}') "
                f"to transaction."
            )
            self.logger.exception(msg)
            raise CacheError(msg) from _e

    def has_table_or_view(self, name, schema: Schema | str):
        if isinstance(schema, Schema):
            schema = schema.get()
        inspector = sa.inspect(self.engine)
        has_table = inspector.has_table(name, schema=schema)
        # workaround for sqlalchemy backends that fail to find views with has_table
        # in particular DuckDB https://github.com/duckdb/duckdb/issues/10322
        if not has_table:
            has_table = name in inspector.get_view_names(schema=schema)
        return has_table

    def _swap_alias_with_table_copy(self, table: Table, table_copy: Table):
        assert table_copy.stage.name == table.stage.name

        schema = self.get_schema(table.stage.transaction_name)
        try:
            self.execute(
                DropAlias(
                    table.name,
                    schema,
                )
            )
            self.execute(
                RenameTable(
                    table_copy.name,
                    table.name,
                    schema,
                )
            )
        except Exception as _e:
            msg = (
                f"Failed putting copied table {table_copy.name} (schema: '{schema}') "
                f"in place of alias."
            )
            raise RuntimeError(msg) from _e

    def _rename_table(self, from_name: str, to_name: str, schema: Schema):
        if from_name == to_name:
            return
        self.logger.info("RENAME", fr=from_name, to=to_name)
        self.execute(RenameTable(from_name, to_name, schema))

    def copy_raw_sql_tables_to_transaction(
        self, metadata: RawSqlMetadata, target_stage: Stage
    ):
        inspector = sa.inspect(self.engine)
        src_schema = self.get_schema(metadata.stage)
        dest_schema = self.get_schema(target_stage.transaction_name)

        # New tables (AND other objects)
        new_objects = set(metadata.new_objects) - set(metadata.prev_objects)
        tables_in_schema = set(inspector.get_table_names(src_schema.get()))
        objects_in_schema = self._get_all_objects_in_schema(src_schema)

        self.check_materialization_details_supported(
            target_stage.materialization_details
        )

        tables_to_copy = new_objects & tables_in_schema
        objects_to_copy = {
            k: v
            for k, v in objects_in_schema.items()
            if k in new_objects and k not in tables_to_copy
        }

        try:
            with self.engine_connect() as conn:
                for name in tables_to_copy:
                    self.execute(
                        CopyTable(
                            name,
                            src_schema,
                            name,
                            dest_schema,
                            unlogged=self.get_unlogged(
                                target_stage.materialization_details
                            ),
                            suffix=self.get_create_table_suffix(
                                target_stage.materialization_details
                            ),
                        ),
                        conn=conn,
                    )
                    self.copy_indexes(
                        name,
                        src_schema,
                        name,
                        dest_schema,
                    )

                for name, obj_metadata in objects_to_copy.items():
                    self._copy_object_to_transaction(
                        name, obj_metadata, src_schema, dest_schema, conn
                    )

        except Exception as _e:
            msg = (
                f"Failed to copy raw sql generated object {name} (schema:"
                f" '{src_schema}') to transaction."
            )
            self.logger.exception(
                msg + " This error is treated as cache-lookup-failure."
            )
            raise CacheError(msg) from _e

    def _get_all_objects_in_schema(self, schema: Schema) -> dict[str, Any]:
        """List all objects that are present in a schema.

        This may include things like views, stored procedures, functions, etc.

        :param schema: the schema
        :return: a dictionary where the keys are the names of the objects, and the
            values are any other (dialect specific) information that might be useful.
        """

        # Because SQLAlchemy only provides limited capabilities to inspect objects
        # inside a schema, we limit ourselves to tables and views.
        inspector = sa.inspect(self.engine)
        tables = inspector.get_table_names(schema.get())
        views = inspector.get_view_names(schema.get())
        return {name: None for name in tables + views}

    def _copy_object_to_transaction(
        self,
        name: str,
        metadata: Any,
        src_schema: Schema,
        dest_schema: Schema,
        conn: sa.Connection,
    ):
        """Copy a generic object from one schema to another.

        :param name: name of the object to be copied
        :param metadata:
            additional information as returned by `get_all_objects_in_schema`
        :param src_schema: the schema in which the object is
        :param dest_schema: the schema into which the object should be copied
        """
        dialect = self.engine.dialect.name
        raise NotImplementedError(f"Not implemented for dialect '{dialect}'.")

    def delete_table_from_transaction(self, table: Table):
        self.execute(
            DropTable(
                table.name,
                self.get_schema(table.stage.transaction_name),
                if_exists=True,
            )
        )

    def store_task_metadata(self, metadata: TaskMetadata, stage: Stage):
        if not self.disable_caching:
            with self.engine_connect() as conn:
                conn.execute(
                    self.tasks_table.insert().values(
                        name=metadata.name,
                        stage=metadata.stage,
                        version=metadata.version,
                        timestamp=metadata.timestamp,
                        run_id=metadata.run_id,
                        position_hash=metadata.position_hash,
                        input_hash=metadata.input_hash,
                        cache_fn_hash=metadata.cache_fn_hash,
                        output_json=metadata.output_json,
                        in_transaction_schema=True,
                    )
                )

    def retrieve_task_metadata(
        self, task: MaterializingTask, input_hash: str, cache_fn_hash: str
    ) -> TaskMetadata:
        if self.disable_caching:
            raise CacheError(
                "Caching is disabled, so we also don't even try to retrieve task"
                f" cache: {task}"
            )

        ignore_cache_function = (
            ConfigContext.get().cache_validation.mode
            == CacheValidationMode.IGNORE_FRESH_INPUT
        )
        try:
            with self.engine_connect() as conn:
                result = (
                    conn.execute(
                        self.tasks_table.select()
                        .where(self.tasks_table.c.name == task.name)
                        .where(self.tasks_table.c.stage == task.stage.name)
                        .where(self.tasks_table.c.version == task.version)
                        .where(self.tasks_table.c.input_hash == input_hash)
                        .where(
                            self.tasks_table.c.cache_fn_hash == cache_fn_hash
                            if not ignore_cache_function
                            else sa.literal(True)
                        )
                        .where(self.tasks_table.c.in_transaction_schema.in_([False]))
                    )
                    .mappings()
                    .one_or_none()
                )
        except sa.exc.MultipleResultsFound:
            raise CacheError("Multiple results found task metadata") from None

        if result is None:
            raise CacheError(f"Couldn't retrieve task from cache: {task}")

        return TaskMetadata(
            name=result.name,
            stage=result.stage,
            version=result.version,
            timestamp=result.timestamp,
            run_id=result.run_id,
            position_hash=result.position_hash,
            input_hash=result.input_hash,
            cache_fn_hash=result.cache_fn_hash,
            output_json=result.output_json,
        )

    def retrieve_all_task_metadata(
        self, task: MaterializingTask, ignore_position_hashes: bool = False
    ) -> list[TaskMetadata]:
        match_condition = sa.and_(
            self.tasks_table.c.name == task.name,
            self.tasks_table.c.stage == task.stage.name,
        )

        if not ignore_position_hashes:
            match_condition = match_condition & (
                self.tasks_table.c.position_hash == task.position_hash
            )
        with self.engine_connect() as conn:
            results = (
                conn.execute(self.tasks_table.select().where(match_condition))
                .mappings()
                .all()
            )
        return [
            TaskMetadata(
                name=result.name,
                stage=result.stage,
                version=result.version,
                timestamp=result.timestamp,
                run_id=result.run_id,
                position_hash=result.position_hash,
                input_hash=result.input_hash,
                cache_fn_hash=result.cache_fn_hash,
                output_json=result.output_json,
            )
            for result in results
        ]

    def store_lazy_table_metadata(self, metadata: LazyTableMetadata):
        if not self.disable_caching:
            with self.engine_connect() as conn:
                conn.execute(
                    self.lazy_cache_table.insert().values(
                        name=metadata.name,
                        stage=metadata.stage,
                        query_hash=metadata.query_hash,
                        task_hash=metadata.task_hash,
                        in_transaction_schema=True,
                    )
                )

    # noinspection DuplicatedCode
    def retrieve_lazy_table_metadata(
        self, query_hash: str, task_hash: str, stage: Stage
    ) -> LazyTableMetadata:
        if self.disable_caching:
            raise CacheError(
                "Caching is disabled, so we also don't even try to retrieve lazy table"
                " cache"
            )

        try:
            with self.engine_connect() as conn:
                result = (
                    conn.execute(
                        self.lazy_cache_table.select()
                        .where(self.lazy_cache_table.c.stage == stage.name)
                        .where(self.lazy_cache_table.c.query_hash == query_hash)
                        .where(self.lazy_cache_table.c.task_hash == task_hash)
                        .where(
                            self.lazy_cache_table.c.in_transaction_schema.in_([False])
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except sa.exc.MultipleResultsFound:
            raise CacheError(
                "Multiple results found for lazy table cache key"
            ) from None

        if result is None:
            raise CacheError("No result found for lazy table cache key")

        return LazyTableMetadata(
            name=result.name,
            stage=result.stage,
            query_hash=result.query_hash,
            task_hash=result.task_hash,
        )

    def store_raw_sql_metadata(self, metadata: RawSqlMetadata):
        if not self.disable_caching:
            with self.engine_connect() as conn:
                conn.execute(
                    self.raw_sql_cache_table.insert().values(
                        prev_objects=json.dumps(metadata.prev_objects),
                        new_objects=json.dumps(metadata.new_objects),
                        stage=metadata.stage,
                        query_hash=metadata.query_hash,
                        task_hash=metadata.task_hash,
                        in_transaction_schema=True,
                    )
                )

    # noinspection DuplicatedCode
    def retrieve_raw_sql_metadata(
        self, query_hash: str, task_hash: str, stage: Stage
    ) -> RawSqlMetadata:
        if self.disable_caching:
            raise CacheError(
                "Caching is disabled, so we also don't even try to retrieve raw sql"
                " cache"
            )

        try:
            with self.engine_connect() as conn:
                result = (
                    conn.execute(
                        self.raw_sql_cache_table.select()
                        .where(self.raw_sql_cache_table.c.stage == stage.name)
                        .where(self.raw_sql_cache_table.c.query_hash == query_hash)
                        .where(self.raw_sql_cache_table.c.task_hash == task_hash)
                        .where(
                            self.raw_sql_cache_table.c.in_transaction_schema.in_(
                                [False]
                            )
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except sa.exc.MultipleResultsFound:
            raise CacheError("Multiple results found for raw sql cache key") from None

        if result is None:
            raise CacheError("No result found for raw sql cache key")

        return RawSqlMetadata(
            prev_objects=json.loads(result.prev_objects),
            new_objects=json.loads(result.new_objects),
            stage=result.stage,
            query_hash=result.query_hash,
            task_hash=result.task_hash,
        )

    def resolve_alias(self, table: Table, stage_name: str | None) -> tuple[str, str]:
        """
        Convert Table objects to string table name and schema.

        Take care of ExternalTableReference objects that turn into Table objects
        with external_schema not None. For normal Table objects, the stage
        schema name is needed. It will be prefixed/postfixed based on config.
        Dialect specific implementations will also try to resolve table aliases
        or synonyms since they may cause trouble with SQLAlchemy.

        :param table: Table object
        :param stage_name: Stage name component to schema (will be different
            naming pattern for currently processed transaction stage)
        :return: string table name and schema
        """
        schema = (
            self.get_schema(stage_name).get()
            if table.external_schema is None
            else table.external_schema
        )
        return table.name, schema

    def get_objects_in_stage(self, stage: Stage):
        schema = self.get_schema(stage.current_name)
        return list(self._get_all_objects_in_schema(schema).keys())

    def get_table_objects_in_stage(self, stage: Stage, include_views=True):
        schema = self.get_schema(stage.current_name).get()
        inspector = sa.inspect(self.engine)

        tables = inspector.get_table_names(schema)
        if not include_views:
            return tables
        views = inspector.get_view_names(schema)
        return [*tables, *views]

    def get_stage_hash(self, stage: Stage) -> str:
        """Compute hash that represents entire stage's output metadata."""
        # We only need to look in tasks_table since the output_json column is updated
        # after evaluating lazy output objects for cache validity. This is the same
        # information we use for producing input_hash of downstream tasks.
        if self.disable_caching:
            raise NotImplementedError(
                "computing stage hash with disabled caching is currently not supported"
            )
        with self.engine_connect() as conn:
            result = conn.execute(
                sa.select(self.tasks_table.c.output_json)
                .where(self.tasks_table.c.stage == stage.name)
                .where(self.tasks_table.c.in_transaction_schema.in_([False]))
            ).scalars()
            result = sorted(result)

        return stable_hash(*result)

    # DatabaseLockManager

    def get_engine_for_locking(self) -> sa.Engine:
        return self._create_engine()

    def get_lock_schema(self) -> Schema:
        return self.get_schema(self.LOCK_SCHEMA)


# Load SQLTableStore Hooks
import pydiverse.pipedag.backend.table.sql.hooks  # noqa

# Load SQLTableStore dialect specific subclasses
import pydiverse.pipedag.backend.table.sql.dialects  # noqa
