from __future__ import annotations

import re
import warnings
from typing import Any

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.sql.elements

from pydiverse.pipedag import Stage, Table
from pydiverse.pipedag.backend.table.base import BaseTableStore, TableHook
from pydiverse.pipedag.backend.table.util.sql_ddl import (
    CopyTable,
    CreateDatabase,
    CreateSchema,
    CreateTableAsSelect,
    CreateViewAsSelect,
    DropFunction,
    DropProcedure,
    DropSchema,
    DropTable,
    DropView,
    PrepareCreateTableAsSelect,
    RenameSchema,
    Schema,
)
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.context.context import ConfigContext, StageCommitTechnique
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.container import RawSql
from pydiverse.pipedag.materialize.core import MaterializingTask
from pydiverse.pipedag.materialize.metadata import (
    LazyTableMetadata,
    RawSqlMetadata,
    TaskMetadata,
)
from pydiverse.pipedag.materialize.util import compute_cache_key


class SQLTableStore(BaseTableStore):
    """Table store that materializes tables to a SQL database

    Uses schema swapping for transactions:
    Creates a schema for each stage and a temporary schema for each
    transaction. If all tasks inside a stage succeed, swaps the schemas by
    renaming them.
    """

    METADATA_SCHEMA = "pipedag_metadata"

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        engine_url = config.pop("url")
        return cls(engine_url, **config)

    def __init__(
        self,
        engine_url: str,
        create_database_if_not_exists: bool = False,
        schema_prefix: str = "",
        schema_suffix: str = "",
        disable_pytsql: bool = False,
        pytsql_isolate_top_level_statements: bool = True,
        print_materialize: bool = False,
        print_sql: bool = False,
        no_db_locking: bool = True,
    ):
        """
        Construct table store.

        :param engine_url: URL for SQLAlchemy engine
        :param create_database_if_not_exists: whether to create database if it does not exist
        :param schema_prefix: prefix string for schemas (dot is interpreted as database.schema)
        :param schema_suffix: suffix string for schemas (dot is interpreted as database.schema)
        :param disable_pytsql: whether to disable the use of pytsql (dialect mssql only)
        :param pytsql_isolate_top_level_statements: forward pytsql executes() parameter
        :param print_materialize: whether to print select statements before materialization
        :param print_sql: whether to print final SQL statements (except for metadata)
        :param no_db_locking: speed up database by telling it we will not rely on it's locking mechanisms
        """
        super().__init__()

        self.create_database_if_not_exists = create_database_if_not_exists
        self.schema_prefix = schema_prefix
        self.schema_suffix = schema_suffix
        self.disable_pytsql = disable_pytsql
        self.pytsql_isolate_top_level_statements = pytsql_isolate_top_level_statements
        self.print_materialize = print_materialize
        self.print_sql = print_sql
        self.no_db_locking = no_db_locking
        self.metadata_schema = self.get_schema(self.METADATA_SCHEMA)

        self._init_database(engine_url, create_database_if_not_exists)
        self.engine = self._connect(engine_url, self.schema_prefix, self.schema_suffix)

        # Set up metadata tables and schema
        from sqlalchemy import BigInteger, Boolean, Column, DateTime, String

        self.sql_metadata = sa.MetaData()

        # Task Table is unique for stage * in_transaction_schema
        self.tasks_table = sa.Table(
            "tasks",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("name", String(128)),
            Column("stage", String(64)),
            Column("version", String(64)),
            Column("timestamp", DateTime),
            Column("run_id", String(32)),
            Column("input_hash", String(32)),
            Column("cache_fn_hash", String(32)),
            Column("output_json", String(2048)),  # 2k might be too small => TBD
            Column("in_transaction_schema", Boolean),
            schema=self.metadata_schema.get(),
        )

        # Lazy Cache Table is unique for stage * in_transaction_schema
        self.lazy_cache_table = sa.Table(
            "lazy_tables",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("name", String(128)),
            Column("stage", String(64)),
            Column("cache_key", String(64)),
            Column("in_transaction_schema", Boolean),
            schema=self.metadata_schema.get(),
        )

        # Sql Cache Table is unique for stage * in_transaction_schema
        self.raw_sql_cache_table = sa.Table(
            "raw_sql_tables",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("prev_tables", String(256)),
            Column("tables", String(256)),
            Column("stage", String(64)),
            Column("cache_key", String(64)),
            Column("in_transaction_schema", Boolean),
            schema=self.metadata_schema.get(),
        )

        # Stage Table is unique for stage
        self.stage_table = sa.Table(
            "stages",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("stage", String(64)),
            Column("cur_transaction_name", String(256)),
            schema=self.metadata_schema.get(),
        )

    @staticmethod
    def _init_database(engine_url: str, create_database_if_not_exists: bool):
        if not create_database_if_not_exists:
            return

        try_engine = sa.create_engine(engine_url)
        if try_engine.dialect.name in ["mssql", "ibm_db_sa"]:
            try_engine.dispose()
            return

        # Attention: this is a really hacky way to create a generic engine for creating a database before one can open a
        #  connection to self.engine_url which references a database and will fail if the database does not exist
        url = sa.engine.make_url(engine_url)

        if try_engine.dialect.name == "postgresql":
            try:
                with try_engine.connect() as conn:
                    # try whether connection with database in connect string works
                    conn.execute("SELECT 1")
            except sa.exc.DBAPIError:
                postgres_db_url = url.set(database="postgres")
                tmp_engine = sa.create_engine(postgres_db_url)
                try:
                    with tmp_engine.connect() as conn:
                        conn.execute("COMMIT")
                        conn.execute(CreateDatabase(url.database))
                except sa.exc.DBAPIError:
                    # This happens if multiple instances try to create the database
                    # at the same time.
                    with try_engine.connect() as conn:
                        # Verify database actually exists
                        conn.execute("SELECT 1")
        else:
            raise NotImplementedError(
                "create_database_if_not_exists is only implemented for postgres, yet"
            )
        try_engine.dispose()

    @staticmethod
    def _connect(engine_url, schema_prefix, schema_suffix):
        engine = sa.create_engine(engine_url)
        if engine.dialect.name == "mssql":
            engine.dispose()
            # this is needed to allow for CREATE DATABASE statements (we don't rely on database transactions anyways)
            engine = sa.create_engine(engine_url, connect_args={"autocommit": True})

        if engine.dialect.name == "mssql":
            if "." in schema_prefix and "." in schema_suffix:
                raise AttributeError(
                    "Config Error: It is not allowed to have a dot in both"
                    " schema_prefix and schema_suffix for SQL Server / mssql database:"
                    f' schema_prefix="{schema_prefix}", schema_suffix="{schema_suffix}"'
                )

            if (schema_prefix + schema_suffix).count(".") != 1:
                raise AttributeError(
                    "Config Error: There must be exactly dot in both schema_prefix and"
                    " schema_suffix together for SQL Server / mssql database:"
                    f' schema_prefix="{schema_prefix}", schema_suffix="{schema_suffix}"'
                )
        return engine

    def get_schema(self, name):
        return Schema(name, self.schema_prefix, self.schema_suffix)

    def execute(self, query, *, conn=None):
        if conn is not None:
            if self.print_sql:
                query_str = (
                    query
                    if isinstance(query, str)
                    else SQLAlchemyTableHook.lazy_query_str(self, query)
                )
                self.logger.info(f"Executing sql:\n{query_str}")
            return conn.execute(query)
        else:
            # TODO: also replace engine.connect() with own context manager that can be used in other places
            with self.engine.connect() as conn:
                # if self.engine.dialect.name == "mssql" and self.no_db_locking:
                #     conn.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
                return self.execute(query, conn=conn)

    def execute_raw_sql(self, raw_sql: RawSql):
        """Executed raw SQL statements in the associated transaction stage"""
        sql = raw_sql.sql
        if self.engine.name == "mssql":
            # noinspection PyBroadException
            if not self.disable_pytsql:
                # known problems for pytsql_isolate_top_level_statements: true:
                #   - conditional DECLARE and SELECT statements are not handled properly
                import pytsql

                if self.print_sql:
                    max_len = 50000  # consider making an option in ConfigContext
                    opt_end = "\n..." if len(sql) > max_len else ""
                    self.logger.info(f"Executing raw sql:\n{sql[0:max_len]}{opt_end}")
                # use pytsql for executing T-SQL scripts containing many GO statements
                pytsql.executes(
                    sql,
                    self.engine,
                    isolate_top_level_statements=self.pytsql_isolate_top_level_statements,
                )
            else:
                # Offer an alternative SQL splitting in case pytsql does not work
                # known problems:
                #   - DECLARE statements will not be available across GO
                last_use = None
                for stmt in re.split(r"\bGO\b", sql, flags=re.IGNORECASE):
                    if stmt.strip() == "":
                        # allow GO at end of script
                        continue
                    if re.match(r"\bUSE\b", stmt.strip()):
                        last_use = stmt
                    with self.engine.connect() as conn:
                        if last_use is not None:
                            self.execute(last_use, conn=conn)
                        self.execute(stmt, conn=conn)
        else:
            for stmt in sql.split(";"):
                self.execute(stmt)

    def setup(self):
        super().setup()
        self.execute(CreateSchema(self.metadata_schema, if_not_exists=True))
        with self.engine.connect() as conn:
            self.sql_metadata.create_all(conn)

    def dispose(self):
        self.engine.dispose()
        super().dispose()

    def init_stage(self, stage: Stage):
        cfg = ConfigContext.get()
        if cfg.stage_commit_technique == StageCommitTechnique.READ_VIEWS:
            # determine whether we need to write to even or odd transaction-schema
            with self.engine.connect() as conn:
                metadata_rows = (
                    conn.execute(
                        self.stage_table.select().where(
                            self.stage_table.c.stage == stage.name
                        )
                    )
                    .mappings()
                    .all()
                )
                cur_transaction_name = ""
                if len(metadata_rows) == 1:
                    cur_transaction_name = metadata_rows[0]["cur_transaction_name"]

                suffix = "__even" if cur_transaction_name.endswith("__odd") else "__odd"
                new_transaction_name = stage.name + suffix
                stage.set_transaction_name(new_transaction_name)

        cs_base = CreateSchema(self.get_schema(stage.name), if_not_exists=True)
        ds_trans = DropSchema(
            self.get_schema(stage.transaction_name), if_exists=True, cascade=True
        )
        cs_trans = CreateSchema(
            self.get_schema(stage.transaction_name), if_not_exists=False
        )

        dialect_supports_drop_cascade = True
        if self.engine.dialect.name == "ibm_db_sa":
            dialect_supports_drop_cascade = False
        if self.engine.dialect.name == "mssql" and "." in self.schema_suffix:
            # TODO: detect whether tmp_schema exists and rename it as base or transaction schema if one is missing
            # tmp_schema = self.get_schema(stage.name + "__swap")
            cs_trans_initial = CreateSchema(
                self.get_schema(stage.transaction_name), if_not_exists=True
            )
            schema = self.get_schema(stage.transaction_name)
            full_name = schema.get()
            database, schema_only = full_name.split(".")

            # don't drop/create databases, just replace the schema underneath (files will keep name on renaming)
            with self.engine.connect() as conn:
                self.execute(cs_base, conn=conn)
                self.execute(cs_trans_initial, conn=conn)
                self.execute(f"USE [{database}]", conn=conn)
                # clear tables in schema
                sql = f"""
                    EXEC sp_MSforeachtable
                      @command1 = 'DROP TABLE ?'
                    , @whereand = 'AND SCHEMA_NAME(schema_id) = ''{schema_only}'' '
                """
                self.execute(sql, conn=conn)
                # clear views and stored procedures
                views = self.get_view_names(full_name)
                other = self.get_mssql_sql_modules(full_name)
                procedures = [
                    name for name, _type in other.items() if _type.strip() == "P"
                ]
                functions = [
                    name for name, _type in other.items() if _type.strip() == "FN"
                ]
                for view in views:
                    # the 'USE [{database}]' statement is important for this call
                    self.execute(DropView(view, schema, if_exists=True), conn=conn)
                for procedure in procedures:
                    # the 'USE [{database}]' statement is important for this call
                    self.execute(
                        DropProcedure(procedure, schema, if_exists=True), conn=conn
                    )
                for function in functions:
                    # the 'USE [{database}]' statement is important for this call
                    self.execute(
                        DropFunction(function, schema, if_exists=True), conn=conn
                    )

                conn.execute(
                    self.tasks_table.delete()
                    .where(self.tasks_table.c.stage == stage.name)
                    .where(self.tasks_table.c.in_transaction_schema.in_([True]))
                )
        else:
            with self.engine.connect() as conn:
                self.execute(cs_base, conn=conn)
                if not dialect_supports_drop_cascade:
                    meta = sa.MetaData()
                    meta.reflect(
                        bind=conn, schema=self.get_schema(stage.transaction_name).get()
                    )
                    for table in meta.tables.values():
                        self.execute(
                            DropTable(
                                table.name, self.get_schema(stage.transaction_name)
                            ),
                            conn=conn,
                        )
                self.execute(ds_trans, conn=conn)
                self.execute(cs_trans, conn=conn)

                conn.execute(
                    self.tasks_table.delete()
                    .where(self.tasks_table.c.stage == stage.name)
                    .where(self.tasks_table.c.in_transaction_schema.in_([True]))
                )

    def commit_stage(self, stage: Stage):
        cfg = ConfigContext.get()
        if cfg.stage_commit_technique == StageCommitTechnique.SCHEMA_SWAP:
            tmp_schema = self.get_schema(stage.name + "__swap")
            # potentially this disposal must be optional since it does not allow for multi-threaded stage execution
            self.engine.dispose()  # dispose open connections which may prevent schema swapping
            with self.engine.connect() as conn:
                with conn.begin():
                    # TODO: for mssql try to find schema does not exist and then move the forgotten tmp schema there
                    self.execute(
                        DropSchema(tmp_schema, if_exists=True, cascade=True), conn=conn
                    )
                    # TODO: in case "." is in self.schema_prefix, we need to implement schema renaming by
                    #  creating the new schema and moving table objects over
                    self.execute(
                        RenameSchema(
                            self.get_schema(stage.name),
                            tmp_schema,
                        ),
                        conn=conn,
                    )
                    self.execute(
                        RenameSchema(
                            self.get_schema(stage.transaction_name),
                            self.get_schema(stage.name),
                        ),
                        conn=conn,
                    )
                    self.execute(
                        RenameSchema(
                            tmp_schema, self.get_schema(stage.transaction_name)
                        ),
                        conn=conn,
                    )

                    for table in [
                        self.tasks_table,
                        self.lazy_cache_table,
                        self.raw_sql_cache_table,
                    ]:
                        conn.execute(
                            table.delete()
                            .where(table.c.stage == stage.name)
                            .where(table.c.in_transaction_schema.in_([False]))
                        )
                        conn.execute(
                            table.update()
                            .where(table.c.stage == stage.name)
                            .values(in_transaction_schema=False)
                        )
        elif cfg.stage_commit_technique == StageCommitTechnique.READ_VIEWS:
            # delete all read views in visible stage
            dest_meta = sa.MetaData()
            dest_meta.reflect(
                bind=self.engine, schema=self.get_schema(stage.name).get()
            )
            # create views for all tables in transaction schema
            src_meta = sa.MetaData()
            src_meta.reflect(
                bind=self.engine, schema=self.get_schema(stage.transaction_name).get()
            )
            with self.engine.connect() as conn:
                with conn.begin():
                    views = self.get_view_names(self.get_schema(stage.name).get())
                    for view in views:
                        self.execute(
                            DropView(view, schema=self.get_schema(stage.name)),
                            conn=conn,
                        )

                    for table in src_meta.tables.values():
                        self.execute(
                            CreateViewAsSelect(
                                table.name,
                                self.get_schema(stage.name),
                                sa.select(table),
                            ),
                            conn=conn,
                        )

                    metadata_rows = (
                        conn.execute(
                            self.stage_table.select().where(
                                self.stage_table.c.stage == stage.name
                            )
                        )
                        .mappings()
                        .all()
                    )
                    if len(metadata_rows) == 0:
                        conn.execute(
                            self.stage_table.insert().values(
                                stage=stage.name,
                                cur_transaction_name=stage.transaction_name,
                            )
                        )
                    else:
                        conn.execute(
                            self.stage_table.update()
                            .where(self.stage_table.c.stage == stage.name)
                            .values(cur_transaction_name=stage.transaction_name)
                        )

        else:
            assert (
                False
            ), f"Unexpected stage_commit_technique: {cfg.stage_commit_technique}"

    def copy_table_to_transaction(self, table: Table):
        stage = table.stage
        if not sa.inspect(self.engine).has_table(
            table.name, schema=self.get_schema(stage.name).get()
        ):
            raise CacheError(
                f"Can't copy table '{table.name}' (schema: '{stage.name}')"
                " to transaction because no such table exists."
            )

        self.execute(
            CopyTable(
                table.name,
                self.get_schema(stage.name),
                table.name,
                self.get_schema(stage.transaction_name),
            )
        )

    def copy_lazy_table_to_transaction(self, metadata: LazyTableMetadata, table: Table):
        if not sa.inspect(self.engine).has_table(
            metadata.name, schema=self.get_schema(metadata.stage).get()
        ):
            raise CacheError(
                f"Can't copy lazy table '{metadata.name}' (schema:"
                f" '{metadata.stage}') to transaction because no such table"
                " exists."
            )

        self.execute(
            CopyTable(
                metadata.name,
                self.get_schema(metadata.stage),
                table.name,
                self.get_schema(table.stage.transaction_name),
            )
        )

    def get_view_names(self, schema: str, *, include_everything=False) -> list[str]:
        inspector = sa.inspect(self.engine)
        if include_everything and self.engine.dialect.name == "mssql":
            return list(self.get_mssql_sql_modules(schema).keys())
        else:
            return inspector.get_view_names(schema)

    # noinspection SqlDialectInspection
    def get_mssql_sql_modules(self, schema: str):
        with self.engine.connect() as conn:
            database, schema_only = schema.split(".")
            self.execute(f"USE {database}", conn=conn)
            # include stored procedures in view names because they can also be recreated
            # based on sys.sql_modules.description
            sql = (
                "select obj.name, obj.type from sys.sql_modules as mod "
                "left join sys.objects as obj on mod.object_id=obj.object_id "
                "left join sys.schemas as schem on schem.schema_id=obj.schema_id "
                f"where schem.name='{schema_only}'"
            )
            rows = self.execute(sql, conn=conn).fetchall()
        return {row[0]: row[1] for row in rows}

    # noinspection SqlDialectInspection
    def copy_raw_sql_tables_to_transaction(
        self, metadata: RawSqlMetadata, target_stage: Stage
    ):
        src_schema = self.get_schema(metadata.stage)
        dest_schema = self.get_schema(target_stage.transaction_name)
        views = set(self.get_view_names(src_schema.get(), include_everything=True))
        for table_name in set(metadata.tables) - set(metadata.prev_tables) - set(views):
            self.execute(
                CopyTable(
                    table_name,
                    src_schema,
                    table_name,
                    dest_schema,
                )
            )
        for table_name in (
            set(metadata.tables) - set(metadata.prev_tables)
        ).intersection(views):
            if self.engine.dialect.name == "mssql":
                src_database, src_schema_only = src_schema.get().split(".")
                dest_database, dest_schema_only = dest_schema.get().split(".")
                with self.engine.connect() as conn:
                    self.execute(f"USE {src_database}", conn=conn)
                    sql = f"""
                        SELECT definition
                        FROM sys.sql_modules
                        WHERE [object_id] = OBJECT_ID('[{src_schema_only}].[{table_name}]');
                    """
                    view_sql = self.execute(sql, conn=conn).fetchone()
                    assert view_sql is not None
                    view_sql = view_sql[0]
                    if src_schema_only != dest_schema_only:
                        for schema_brackets in [True, False]:
                            for table_brackets in [True, False]:
                                schema = (
                                    f"[{src_schema_only}]"
                                    if schema_brackets
                                    else src_schema_only
                                )
                                table = (
                                    f"[{table_name}]" if table_brackets else table_name
                                )
                                view_sql = view_sql.replace(
                                    f"{schema}.{table}",
                                    f"[{dest_schema_only}].[{table_name}]",
                                )
                    self.execute(f"USE {dest_database}", conn=conn)
                    self.execute(view_sql, conn=conn)
            else:
                raise NotImplementedError("Not yet implemented")

    def delete_table_from_transaction(self, table: Table):
        self.execute(
            DropTable(
                table.name,
                self.get_schema(table.stage.transaction_name),
                if_exists=True,
            )
        )

    def store_task_metadata(self, metadata: TaskMetadata, stage: Stage):
        with self.engine.connect() as conn:
            conn.execute(
                self.tasks_table.insert().values(
                    name=metadata.name,
                    stage=metadata.stage,
                    version=metadata.version,
                    timestamp=metadata.timestamp,
                    run_id=metadata.run_id,
                    input_hash=metadata.input_hash,
                    cache_fn_hash=metadata.cache_fn_hash,
                    output_json=metadata.output_json,
                    in_transaction_schema=True,
                )
            )

    # noinspection DuplicatedCode
    def retrieve_task_metadata(self, task: MaterializingTask) -> TaskMetadata:
        ignore_fresh_input = RunContext.get().ignore_fresh_input
        try:
            with self.engine.connect() as conn:
                result = (
                    conn.execute(
                        self.tasks_table.select()
                        .where(self.tasks_table.c.name == task.name)
                        .where(self.tasks_table.c.stage == task.stage.name)
                        .where(self.tasks_table.c.version == task.version)
                        .where(self.tasks_table.c.input_hash == task.input_hash)
                        .where(
                            self.tasks_table.c.cache_fn_hash == task.cache_fn_hash
                            if not ignore_fresh_input
                            else sa.literal(True)
                        )
                        .where(self.tasks_table.c.in_transaction_schema.in_([False]))
                    )
                    .mappings()
                    .one_or_none()
                )
        except sa.exc.MultipleResultsFound:
            raise CacheError("Multiple results found task metadata")

        if result is None:
            raise CacheError(f"Couldn't retrieve task from cache: {task}")

        return TaskMetadata(
            name=result.name,
            stage=result.stage,
            version=result.version,
            timestamp=result.timestamp,
            run_id=result.run_id,
            input_hash=result.input_hash,
            cache_fn_hash=result.cache_fn_hash,
            output_json=result.output_json,
        )

    def store_lazy_table_metadata(self, metadata: LazyTableMetadata):
        with self.engine.connect() as conn:
            conn.execute(
                self.lazy_cache_table.insert().values(
                    name=metadata.name,
                    stage=metadata.stage,
                    cache_key=metadata.cache_key,
                    in_transaction_schema=True,
                )
            )

    def retrieve_lazy_table_metadata(
        self, cache_key: str, stage: Stage
    ) -> LazyTableMetadata:
        # noinspection PyUnresolvedReferences,DuplicatedCode
        try:
            with self.engine.connect() as conn:
                result = (
                    conn.execute(
                        self.lazy_cache_table.select()
                        .where(self.lazy_cache_table.c.stage == stage.name)
                        .where(self.lazy_cache_table.c.cache_key == cache_key)
                        .where(
                            self.lazy_cache_table.c.in_transaction_schema.in_([False])
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except sa.exc.MultipleResultsFound:
            raise CacheError("Multiple results found for lazy table cache key")

        if result is None:
            raise CacheError("No result found for lazy table cache key")

        return LazyTableMetadata(
            name=result.name,
            stage=result.stage,
            cache_key=result.cache_key,
        )

    def store_raw_sql_metadata(self, metadata: RawSqlMetadata):
        with self.engine.connect() as conn:
            conn.execute(
                self.raw_sql_cache_table.insert().values(
                    # ToDo: consider quoting or array column
                    prev_tables=";".join(metadata.prev_tables),
                    tables=";".join(metadata.tables),
                    stage=metadata.stage,
                    cache_key=metadata.cache_key,
                    in_transaction_schema=True,
                )
            )

    def retrieve_raw_sql_metadata(self, cache_key: str, stage: Stage) -> RawSqlMetadata:
        # noinspection PyUnresolvedReferences,DuplicatedCode
        try:
            with self.engine.connect() as conn:
                result = (
                    conn.execute(
                        self.raw_sql_cache_table.select()
                        .where(self.raw_sql_cache_table.c.stage == stage.name)
                        .where(self.raw_sql_cache_table.c.cache_key == cache_key)
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
            raise CacheError("Multiple results found for lazy table cache key")

        if result is None:
            raise CacheError("No result found for lazy table cache key")

        return RawSqlMetadata(
            prev_tables=result.prev_tables.split(";"),
            tables=result.tables.split(";"),
            stage=result.stage,
            cache_key=result.cache_key,
        )

    def get_stage_hash(self, stage: Stage):
        query = (
            sa.select(self.tasks_table.c.output_json)
            .where(
                (self.tasks_table.c.stage == stage.name)
                & ~self.tasks_table.c.in_transaction_schema
            )
            .order_by(self.tasks_table.c.output_json)
        )
        return compute_cache_key(
            *[
                output_json
                for output_json in pd.read_sql(query, con=self.engine).iloc[0]
            ]
        )


@SQLTableStore.register_table()
class SQLAlchemyTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, (sa.Table, sa.sql.Select, sa.sql.elements.TextClause))

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == sa.Table

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[sa.sql.elements.TextClause | sa.Text],
        stage_name,
    ):
        obj = table.obj
        if isinstance(table.obj, sa.Table):
            obj = sa.select(table.obj)
        if store.print_materialize:
            query_str = cls.lazy_query_str(store, obj)
            store.logger.info(
                f"Executing CREATE TABLE AS SELECT ({table}):\n{query_str}"
            )
        else:
            store.logger.info(f"Executing CREATE TABLE AS SELECT ({table})")
        if store.engine.dialect.name == "ibm_db_sa":
            # DB2 needs CREATE TABLE & INSERT INTO statements
            store.execute(
                PrepareCreateTableAsSelect(
                    table.name, store.get_schema(stage_name), obj
                )
            )
        store.execute(
            CreateTableAsSelect(table.name, store.get_schema(stage_name), obj)
        )

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        return sa.Table(
            table.name,
            sa.MetaData(bind=store.engine),
            schema=store.get_schema(stage_name).get(),
            autoload_with=store.engine,
        )

    @classmethod
    def lazy_query_str(cls, store, obj) -> str:
        return str(obj.compile(store.engine, compile_kwargs={"literal_binds": True}))

    @classmethod
    def list_tables(cls, store, stage_name, *, include_everything=False):
        inspector = sa.inspect(store.engine)
        schema = store.get_schema(stage_name).get()
        return inspector.get_table_names(schema) + store.get_view_names(
            schema, include_everything=include_everything
        )


@SQLTableStore.register_table(pd)
class PandasTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pd.DataFrame)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == pd.DataFrame

    @classmethod
    def materialize(cls, store, table: Table[pd.DataFrame], stage_name):
        schema = store.get_schema(stage_name).get()
        if store.print_materialize:
            store.logger.info(f"Writing table '{schema}.{table.name}':\n{table.obj}")
        table.obj.to_sql(
            table.name,
            store.engine,
            schema=schema,
            index=False,
        )

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        with store.engine.connect() as conn:
            df = pd.read_sql_table(
                table.name, conn, schema=store.get_schema(stage_name).get()
            )
            return df

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        if name := obj.attrs.get("name"):
            return Table(obj, name)
        return super().auto_table(obj)

    @classmethod
    def list_tables(cls, store, stage_name):
        return SQLAlchemyTableHook.list_tables(store, stage_name)


try:
    import pydiverse.transform as pdt
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    pdt = None


# noinspection PyUnresolvedReferences,PyProtectedMember
@SQLTableStore.register_table(pdt)
class PydiverseTransformTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pdt.Table)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        return issubclass(type_, (PandasTableImpl, SQLTableImpl))

    @classmethod
    def materialize(cls, store, table: Table[pdt.Table], stage_name):
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        t = table.obj
        if isinstance(t._impl, PandasTableImpl):
            from pydiverse.transform.core.verbs import collect

            table.obj = t >> collect()
            # noinspection PyTypeChecker
            return PandasTableHook.materialize(store, table, stage_name)
        if isinstance(t._impl, SQLTableImpl):
            table.obj = t._impl.build_select()
            # noinspection PyTypeChecker
            return SQLAlchemyTableHook.materialize(store, table, stage_name)
        raise NotImplementedError

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        if issubclass(as_type, PandasTableImpl):
            df = PandasTableHook.retrieve(store, table, stage_name, pd.DataFrame)
            return pdt.Table(PandasTableImpl(table.name, df))
        if issubclass(as_type, SQLTableImpl):
            sa_tbl = SQLAlchemyTableHook.retrieve(store, table, stage_name, sa.Table)
            return pdt.Table(SQLTableImpl(store.engine, sa_tbl))
        raise NotImplementedError

    @classmethod
    def auto_table(cls, obj: pdt.Table):
        return Table(obj, obj._impl.name)

    @classmethod
    def lazy_query_str(cls, store, obj: pdt.Table) -> str:
        from pydiverse.transform.core.verbs import build_query

        query = obj >> build_query()

        if query is not None:
            return str(query)
        return super().lazy_query_str(store, obj)

    @classmethod
    def list_tables(cls, store, stage_name):
        return SQLAlchemyTableHook.list_tables(store, stage_name)
