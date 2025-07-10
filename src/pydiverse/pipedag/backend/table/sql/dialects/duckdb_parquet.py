# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import os
import shutil
from typing import Any

import sqlalchemy as sa
from upath import UPath

import pydiverse.pipedag.backend.table.sql.hooks as sql_hooks
from pydiverse.pipedag import ConfigContext, Schema, Stage, Table
from pydiverse.pipedag.backend.table.sql.ddl import (
    CopySelectTo,
    CreateViewAsSelect,
    DropView,
)
from pydiverse.pipedag.backend.table.sql.dialects.duckdb import DuckDBTableStore
from pydiverse.pipedag.backend.table.sql.sql import DISABLE_DIALECT_REGISTRATION
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.materialize.table_hook_base import (
    AutoVersionSupport,
    CanMatResult,
    CanRetResult,
    TableHook,
)
from pydiverse.pipedag.util import normalize_name
from pydiverse.pipedag.util.path import is_file_uri

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import duckdb
except ImportError:
    duckdb = None

try:
    from sqlalchemy import Select, TextClause
    from sqlalchemy import Text as SqlText
except ImportError:
    # For compatibility with sqlalchemy < 2.0
    from sqlalchemy.sql.expression import TextClause
    from sqlalchemy.sql.selectable import Select

    SqlText = TextClause  # this is what sa.text() returns


class ParquetTableStore(DuckDBTableStore):
    """Table store that materializes tables as parquet files.

    Additionally, a duckdb file is created which links to parquet files.

    Uses schema swapping for transactions:
    Creates a schema for each stage and a temporary schema for each
    transaction. If all tasks inside a stage succeed, swaps the schemas by
    renaming them.

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
    :param parquet_base_bath:
        This is an fsspec compatible path to either a directory or an S3 bucket key
        prefix. Examples are '/tmp/pipedag/parquet/' or 's3://pipedag-test-bucket/table_store/'.
        `instance_id` will automatically be appended to parquet_base_bath.

    """

    _dialect_name = DISABLE_DIALECT_REGISTRATION

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        config = config.copy()
        instance_id = normalize_name(ConfigContext.get().instance_id)
        config["parquet_base_path"] = UPath(config["parquet_base_path"]) / instance_id
        return super()._init_conf_(config)

    def __init__(
        self,
        engine_url: str,
        parquet_base_path: UPath,
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
        super().__init__(
            engine_url,
            create_database_if_not_exists=create_database_if_not_exists,
            schema_prefix=schema_prefix,
            schema_suffix=schema_suffix,
            avoid_drop_create_schema=avoid_drop_create_schema,
            print_materialize=print_materialize,
            print_sql=print_sql,
            no_db_locking=no_db_locking,
            strict_materialization_details=strict_materialization_details,
            materialization_details=materialization_details,
            default_materialization_details=default_materialization_details,
            max_concurrent_copy_operations=max_concurrent_copy_operations,
            sqlalchemy_pool_size=sqlalchemy_pool_size,
            sqlalchemy_pool_timeout=sqlalchemy_pool_timeout,
        )
        self.parquet_base_path = parquet_base_path

        # ## state

        # whether parquet files of stage are stored in __odd or __even
        # can typically be found in metadata, but firstly we cache this
        # and secondly we modify it for the current stage before commit
        self.parquet_schema_paths: dict[str, UPath] = {}
        # tables may get alias to other transaction slot in case of cache
        # valid tasks
        self.parquet_table_paths: dict[tuple[str, str], UPath] = {}
        # remember tables that need to be copied if stage is not unchanged
        self.parquet_deferred_copy: list[Table] = []

        self.logger.info(
            "Initialized Parquet Table Store",
            parquet_base_path=self.parquet_base_path,
            engine_url=self.engine_url.render_as_string(hide_password=True),
            schema_prefix=self.schema_prefix,
            schema_suffix=self.schema_suffix,
        )

    def _create_engine(self):
        engine = super()._create_engine(
            connect_args={
                # configure s3 for minio (see docker-compose.yaml)
                "config": {
                    "s3_endpoint": "localhost:9000",
                    "s3_url_style": "path",
                    "s3_use_ssl": "false",
                    # "s3_region": "us-east-1",
                    "s3_access_key_id": "minioadmin",
                    "s3_secret_access_key": "minioadmin",
                },
            }
        )

        def execute(con, statement):
            """
            Execute a SQL statement and return the result.
            """
            if sa.__version__ >= "2.0.0":
                return con.execute(sa.text(statement))
            else:
                return con.execute(statement)

        with engine.connect() as con:
            # Enable HTTPFS extension
            execute(con, "INSTALL httpfs;")
            execute(con, "LOAD httpfs;")

            # MinIO credentials (default unless you've changed them)
            execute(con, "SET s3_access_key_id='minioadmin';")
            execute(con, "SET s3_secret_access_key='minioadmin';")

            # MinIO-specific settings
            execute(con, "SET s3_use_ssl=false;")
            execute(con, "SET s3_endpoint='localhost:9000';")  # MinIO server URL
            execute(con, "SET s3_url_style='path';")  # Important: MinIO uses path-style URLs

            if sa.__version__ >= "2.0.0":
                con.commit()

        return engine

    def init_stage(self, stage: Stage):
        # fetch existing tables from database before schema is deleted there
        old_transaction_name = self._get_read_view_transaction_name(stage.name)
        new_transaction_name = self._get_read_view_original_transaction_name(stage, old_transaction_name)
        with self.engine_connect() as conn:
            existing_tables = self.execute(
                f"FROM duckdb_views() SELECT view_name WHERE "
                f"schema_name='{new_transaction_name}' "
                f"and sql like '%%FROM read_parquet(%%'",
                conn=conn,
            ).fetchall()
        # this creates transaction schema and modifies stage.current_name
        super().init_stage(stage)
        # update cache: this is important since before this stage commits,
        # self._get_read_view_transaction_name() will yield the wrong name
        new_schema = self.get_schema(stage.current_name)
        self.parquet_schema_paths[new_schema.name] = self.get_parquet_path(new_schema)
        # The stage.name should still point to opposite transaction schema
        old_schema = self.get_schema(self._get_read_view_original_transaction_name(stage))
        self.parquet_schema_paths[self.get_schema(stage.name).name] = self.get_parquet_path(old_schema)
        # TODO: for nested stages this might be per stage
        self.parquet_deferred_copy.clear()
        path = self.get_stage_path(stage)
        if is_file_uri(path):
            os.makedirs(path, exist_ok=True)
        for row in existing_tables:
            table = row[0]
            file_path = path / (table + ".parquet")
            try:
                self.logger.info(
                    "Cleaning up parquet file in transaction schema",
                    file_path=file_path,
                )
                file_path.unlink()
            except FileNotFoundError:
                self.logger.error(
                    "Could not remove parquet file while deleting corresponding view in transaction schema",
                    file=file_path,
                    view_name=table,
                    stage=stage,
                )

    def _copy_table(self, table: Table, from_schema: Schema, from_name: str):
        if table.name != from_name:
            # we ignore the _copy_table which wants to write __copy tables
            return
        _ = from_schema  # not used because this only points to READ VIEW
        dest_schema = self.get_schema(table.stage.current_name)
        original_transaction_name = self._get_read_view_original_transaction_name(table.stage)
        original_schema = self.get_schema(original_transaction_name)
        src_file_path = self.get_parquet_path(original_schema) / (from_name + ".parquet")
        dest_file_path = self.get_parquet_path(dest_schema) / src_file_path.name
        self.logger.info(
            "Copying table between transactions",
            src_file_path=src_file_path,
            dest_file_path=dest_file_path,
        )
        if is_file_uri(src_file_path):
            shutil.copy(src_file_path, dest_file_path)
        else:
            src_file_path.fs.copy(src_file_path.as_uri(), dest_file_path.as_uri(), on_error="raise")
        # create view in duckdb database file
        self.execute(CreateViewAsSelect(table.name, dest_schema, self._read_parquet_query(dest_file_path)))

    @staticmethod
    def _read_parquet_query(file_path):
        # attention: we filter for %%FROM read_parquet(%% in init_stage
        return sa.text(f"FROM read_parquet('{file_path}')")

    def _swap_alias_with_table_copy(self, table: Table, table_copy: Table):
        # There is no __copy table which can be renamed for this table store.
        # Alias will be replaced in commit_stage()
        pass

    def _deferred_copy_table(
        self,
        table: Table,
        from_schema: Schema,
        from_name: str,
    ):
        # this will just create an alias in the duckdb database
        super()._deferred_copy_table(table, from_schema, from_name)
        # keep symlink to original parquet file until stage commits
        original_transaction_name = self._get_read_view_original_transaction_name(table.stage)
        original_schema = self.get_schema(original_transaction_name)
        schema = self.get_schema(table.stage.current_name)
        self.parquet_table_paths[(schema.name, table.name)] = self.get_parquet_path(original_schema) / (
            table.name + ".parquet"
        )
        self.parquet_deferred_copy.append(table)

    def commit_stage(self, stage: Stage):
        schema = self.get_schema(stage.current_name)
        stage_transaction_path = self.parquet_schema_paths[schema.name]
        # deferred copy of parquet files if stage is not 100% cache valid
        if RunContext.get().has_stage_changed(stage):
            for table in self.parquet_deferred_copy:
                # remove views which were placed as aliases
                self.execute(DropView(table.name, schema))
                src_file_path = self.parquet_table_paths[(schema.name, table.name)]
                dest_file_path = stage_transaction_path / src_file_path.name
                self.logger.info(
                    "Copying table between transactions",
                    src_file_path=src_file_path,
                    dest_file_path=dest_file_path,
                )
                if is_file_uri(src_file_path):
                    shutil.copy(src_file_path, dest_file_path)
                else:
                    src_file_path.fs.copy(src_file_path.as_uri(), dest_file_path.as_uri(), on_error="raise")
                # replace view in duckdb database file (which was previously just
                # alias to main schema)
                self.execute(CreateViewAsSelect(table.name, schema, self._read_parquet_query(dest_file_path)))
            # switch committed transaction path to the new transaction path
            self.parquet_schema_paths[self.get_schema(stage.name).name] = stage_transaction_path
        super().commit_stage(stage)
        # clear table individual parquet paths since this is not needed after commit
        self.parquet_table_paths = {}

    def _committed_unchanged(self, stage):
        super()._committed_unchanged(stage)
        # transaction schema is discarded, so we don't need to copy tables there
        self.parquet_deferred_copy.clear()

    def get_parquet_path(self, schema: Schema):
        return self.parquet_base_path / schema.get()

    def get_parquet_schema_path(self, schema: Schema) -> UPath:
        # Parquet files are stored in transaction schema while stage.current_name
        # changes to final schema. We resolve a level of indirection in memory.
        if schema.name in self.parquet_schema_paths:
            return self.parquet_schema_paths[schema.name]
        # now we assume schema.name == stage.name (any stage with
        # current_name != name should be in self.parquet_schema_paths)
        transaction_name = self._get_read_view_transaction_name(schema.name)
        if transaction_name == "":
            transaction_name = schema.name
        path = self.get_parquet_path(self.get_schema(transaction_name))
        self.parquet_schema_paths[schema.name] = path
        return path

    def get_stage_path(self, stage: Stage) -> UPath:
        store = ConfigContext.get().store.table_store
        return self.get_parquet_schema_path(store.get_schema(stage.current_name))

    def get_table_path(
        self,
        table: Table,
        file_extension: str = ".parquet",
        lookup_schema: bool = False,
    ) -> UPath:
        store = ConfigContext.get().store.table_store
        schema_name = table.stage.current_name
        return self.get_table_schema_path(table.name, store.get_schema(schema_name), file_extension)

    def get_table_schema_path(self, table_name: str, schema: Schema, file_extension: str = ".parquet") -> UPath:
        # Parquet files might be stored in other transaction schema in case of cache
        # validity. We resolve a level of indirection in memory.
        if (schema.name, table_name) in self.parquet_table_paths:
            return self.parquet_table_paths[(schema.name, table_name)]
        return self.get_parquet_schema_path(schema) / (table_name + file_extension)

    def delete_table_from_transaction(self, table: Table, *, schema: Schema | None = None):
        if schema is None:
            schema = self.get_schema(table.stage.transaction_name)
        file_path = self.get_table_schema_path(table.name, schema)
        file_path.unlink(missing_ok=True)
        self.execute(
            DropView(
                table.name,
                self.get_schema(table.stage.transaction_name),
                if_exists=True,
            )
        )

    def add_primary_key(
        self,
        table_name: str,
        schema: Schema,
        key_columns: list[str],
        *,
        name: str | None = None,
    ):
        # ParquetTableStore does not support indexes. They are ignored.
        pass

    def add_index(
        self,
        table_name: str,
        schema: Schema,
        index_columns: list[str],
        name: str | None = None,
    ):
        # ParquetTableStore does not support indexes. They are ignored.
        pass

    def copy_indexes(self, src_table: str, src_schema: Schema, dest_table: str, dest_schema: Schema):
        # ParquetTableStore does not support indexes. They are ignored.
        pass

    def rename_table(self, table: Table, to_name: str, schema: Schema):
        _ = schema  # ignore given schema
        schema = self.get_schema(table.stage.transaction_name)
        from_path = self.get_table_schema_path(table.name, schema)
        to_path = self.get_table_schema_path(to_name, schema)
        if is_file_uri(from_path):
            os.rename(from_path, to_path)
        else:
            from_path.fs.copy(from_path.as_uri(), to_path.as_uri(), on_error="raise")
            from_path.unlink()

        # drop view
        self.execute(
            DropView(
                table.name,
                schema,
            )
        )
        # create view again
        self.execute(CreateViewAsSelect(to_name, schema, self._read_parquet_query(to_path)))

    def write_subquery(
        self,
        query,
        to_name,
        neighbor_table: Table,
        *,
        unlogged: bool = False,
        suffix: str | None = None,
    ):
        """Write a query to a table in same schema as neighbor_table.

        This is mainly for overriding by derived table stores like ParquetTableStore.
        """
        _ = unlogged, suffix  # materialization details not used by ParquetTableStore
        schema = self.get_schema(neighbor_table.stage.transaction_name)
        file_path = self.get_table_schema_path(to_name, schema)
        self.execute(
            [
                CopySelectTo(
                    file_path,
                    "PARQUET",
                    query,
                ),
                CreateViewAsSelect(to_name, schema, self._read_parquet_query(file_path)),
            ]
        )
        return schema

    def drop_subquery_table(
        self,
        drop_name: str,
        schema: Schema | str,
        neighbor_table: Table,
        if_exists: bool = False,
        cascade: bool = False,
    ):
        """Drop a table in same schema as neighbor_table.

        This is mainly for overriding by derived table stores like ParquetTableStore.
        """
        _ = schema  # ignore given schema; use neighbor_table schema instead
        _ = cascade
        schema = self.get_schema(neighbor_table.stage.transaction_name)
        drop_path = self.get_table_schema_path(drop_name, schema)
        try:
            drop_path.unlink()
        except FileNotFoundError as e:
            if not if_exists:
                raise e
        # drop view
        self.execute(
            DropView(
                drop_name,
                schema,
            )
        )


@ParquetTableStore.register_table(pd)
class PandasTableHook(TableHook[ParquetTableStore]):
    auto_version_support = AutoVersionSupport.TRACE

    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        return CanMatResult.new(issubclass(type_, pd.DataFrame))

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        return CanRetResult.new(issubclass(type_, pd.DataFrame))

    @classmethod
    def materialize(
        cls,
        store: ParquetTableStore,
        table: Table[pd.DataFrame],
        stage_name: str,
        without_config_context: bool = False,
    ):
        file_path = store.get_table_path(table)
        schema = store.get_schema(table.stage.current_name)

        if store.print_materialize:
            store.logger.info(
                f"Writing pandas table '{schema.get()}.{table.name}'",
                file_path=file_path,
                table_obj=table.obj,
            )

        df = table.obj
        df.to_parquet(file_path)
        store.execute(CreateViewAsSelect(table.name, schema, store._read_parquet_query(file_path)))

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type,
        limit: int | None = None,
    ):
        path = store.get_table_path(table, lookup_schema=True)
        import pyarrow.dataset as ds

        if limit is not None:
            df = ds.dataset(path).scanner().head(limit).to_pandas()
        else:
            df = pd.read_parquet(path)
        import pyarrow.parquet

        schema = pyarrow.parquet.read_schema(path)
        df = df.astype(
            {
                col: "datetime64[s]"
                for col, type_, dtype in zip(schema.names, schema.types, df.dtypes)
                if type_ == "date32" and dtype == object  # noqa: E721
            }
        )
        if isinstance(as_type, tuple) and len(as_type) == 2 and len(schema.names) > 0:
            if as_type[1] == "arrow" and not all(
                hasattr(dtype, "storage") and dtype.storage == "pyarrow" for dtype in df.dtypes
            ):
                store.logger.warning(
                    f"Ignoring storage specialization '{as_type[1]}' for {table.name} "
                    f"as ParquetTableStore reads pandas the same encoding as it writes"
                    f" it. This can be changed if need arises.",
                    as_type=as_type,
                    dtypes=df.dtypes,
                )
            if as_type[1] == "numpy" and any(
                hasattr(dtype, "storage") and dtype.storage == "pyarrow" for dtype in df.dtypes
            ):
                store.logger.warning(
                    f"Ignoring storage specialization '{as_type[1]}' for {table.name} "
                    f"as ParquetTableStore reads pandas the same encoding as it writes"
                    f" it. This can be changed if need arises.",
                    as_type=as_type,
                    dtypes=df.dtypes,
                )
        return df

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        return sql_hooks.PandasTableHook.auto_table(obj)

    @classmethod
    def get_computation_tracer(cls):
        return sql_hooks.PandasTableHook.ComputationTracer()


try:
    import polars as pl
except ImportError:
    pl = None


@ParquetTableStore.register_table(pl, duckdb)
class PolarsTableHook(sql_hooks.PolarsTableHook):
    def _execute_materialize_polars(table, store, stage_name):
        _ = stage_name
        file_path = store.get_table_path(table)
        schema = store.get_schema(table.stage.current_name)
        df = table.obj

        if store.print_materialize:
            store.logger.info(
                f"Writing polars table '{schema.get()}.{table.name}' to parquet",
                file_path=file_path,
            )
        df.write_parquet(file_path)
        store.execute(CreateViewAsSelect(table.name, schema, store._read_parquet_query(file_path)))

    @classmethod
    def _execute_query(
        cls,
        store: ParquetTableStore,
        table: Table,
        stage_name: str,
        as_type: type,
        limit: int | None = None,
    ) -> pl.DataFrame:
        _ = as_type
        file_path = store.get_table_path(table)
        df = pl.read_parquet(file_path, n_rows=limit)
        return df


@ParquetTableStore.register_table(pl)
class LazyPolarsTableHook(sql_hooks.LazyPolarsTableHook):
    pass


@ParquetTableStore.register_table()
class SQLAlchemyTableHook(sql_hooks.SQLAlchemyTableHook):
    @classmethod
    def _create_as_select_statements(
        cls,
        table_name: str,
        schema: Schema,
        query: Select | TextClause | SqlText,
        store: ParquetTableStore,
        suffix: str,
        unlogged: bool,
    ):
        file_path = store.get_table_schema_path(table_name, schema)
        return [
            CopySelectTo(
                file_path,
                "PARQUET",
                query,
            ),
            CreateViewAsSelect(table_name, schema, store._read_parquet_query(file_path)),
        ]
