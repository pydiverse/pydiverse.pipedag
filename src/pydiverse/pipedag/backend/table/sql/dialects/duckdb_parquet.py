# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import os
import shutil
import uuid
from typing import Any, Iterable, Literal

import duckdb
import fsspec
import pandas as pd
import polars as pl
import sqlalchemy as sa
from upath import UPath

import pydiverse.pipedag.backend.table.sql.hooks as sql_hooks
from pydiverse.pipedag import ConfigContext, Schema, Stage, Table
from pydiverse.pipedag.backend.table.sql.ddl import (
    CopySelectTo,
    CreateSchema,
    CreateViewAsSelect,
    DropView,
)
from pydiverse.pipedag.backend.table.sql.dialects.duckdb import DuckDBTableStore
from pydiverse.pipedag.backend.table.sql.sql import DISABLE_DIALECT_REGISTRATION
from pydiverse.pipedag.container import SortOrder, View
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.materialize.store import BaseTableStore
from pydiverse.pipedag.materialize.table_hook_base import (
    AutoVersionSupport,
    CanMatResult,
    CanRetResult,
    TableHook,
)
from pydiverse.pipedag.optional_dependency.sqlalchemy import Select, SqlText, TextClause
from pydiverse.pipedag.util.path import is_file_uri


class ParquetTableStore(DuckDBTableStore):
    """Table store that materializes tables as parquet files.

    Additionally, a duckdb file is created which links to parquet files.

    Uses schema swapping for transactions:
    Creates a schema for each stage and a temporary schema for each
    transaction. If all tasks inside a stage succeed, swaps the schemas by
    renaming them.

    .. rubric:: Supported Tables

    The ParquetTableStore can materialize (store task output)
    and dematerialize (retrieve task input) the following Table types:

    .. list-table::
       :widths: 30, 30, 30
       :header-rows: 1
       :stub-columns: 1

       * - Framework
         - Materialization
         - Dematerialization

       * - SQLAlchemy
         - | :py:class:`sa.sql.expression.Selectable <sqlalchemy.sql.expression.Selectable>`
           | :py:class:`sa.sql.expression.TextClause <sqlalchemy.sql.expression.TextClause>`
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
         - | ``pdt.Polars``
           | ``pdt.SqlAlchemy``

       * - pydiverse.pipedag table reference
         - :py:class:`~.ExternalTableReference` (no materialization)
         - Can be read with all dematerialization methods above

       * - pydiverse.pipedag view
         - :py:class:`~.View` (view with support for src union, column renaming, and sorting)
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
        Prefix placed in front of all schema names created by pipedag.
    :param schema_suffix:
        Suffix placed behind all schema names created by pipedag.
    :param avoid_drop_create_schema:
        If ``True``, no ``CREATE SCHEMA`` or ``DROP SCHEMA`` statements get issued.
        This is mostly relevant for databases that support automatic schema
        creation like IBM DB2.

    :param print_materialize:
        If ``True``, log every table materialization.
    :param print_sql:
        If ``True``, log every executed SQL statement.
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
        the table store. See subclasses of :py:class:`BaseMaterializationDetails`
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
    :param force_transaction_suffix:
        This option is for use without proper pipedag flow. It disables lookup in stage
        metadata table for determining correct transaction slot suffix.
        (default: None)
    :param parquet_base_bath:
        This is an fsspec compatible path to either a directory or an S3 bucket key
        prefix. Examples are ``/tmp/pipedag/parquet/`` or ``s3://pipedag-test-bucket/table_store/``.
        ``instance_id`` will automatically be appended to parquet_base_bath.
    :param s3_endpoint_url:
        When using a non-standard S3 endpoint (like minio), this can be used to specify the URL.
        Unfortunately, the AWS_ENDPOINT_URL environment variable is not automatically picked up by duckdb.
        (default: None)
    :param s3_use_ssl:
        Whether to use SSL when connecting to S3 endpoint. (default: None)
    :param s3_url_style:
        Specify URL style when connecting to S3 endpoint. Minio for example requires s3_url_style='path'.
        (default: None)
    :param allow_overwrite:
        Allow overwriting tables with store_table.
        (default: False)
    """

    _dialect_name = DISABLE_DIALECT_REGISTRATION

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        config = config.copy()
        config["parquet_base_path"] = UPath(config["parquet_base_path"])
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
        force_transaction_suffix: str | None = None,
        s3_endpoint_url: str | None = None,
        s3_url_style: str | None = None,
        s3_region: str | None = None,
        allow_overwrite: bool = False,
    ):
        # must be initialized before super().__init__() call because self._create_engine() is called there
        self.parquet_base_path = parquet_base_path
        self.s3_endpoint_url = s3_endpoint_url
        self.s3_url_style = s3_url_style
        self.s3_region = s3_region
        self.allow_overwrite = allow_overwrite
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
            force_transaction_suffix=force_transaction_suffix,
        )

        # Add parquet specific metadata table for user id.
        # This can be UUID which is constant per .duckdb file and used to sync with metadata_store.
        self.sql_metadata_local = sa.MetaData(schema=self.metadata_schema.get())
        self.user_id_table = sa.Table(
            "user_id",
            self.sql_metadata_local,
            sa.Column("user_id", sa.String(64)),
        )
        self.sync_views_table = None

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

    def set_metadata_store(self, store: BaseTableStore):
        super().set_metadata_store(store)

        # make sure sync_views_table exists in metadata_store
        self.sync_views_table = sa.Table(
            "sync_views",
            store.sql_metadata,
            sa.Column("schema", sa.String(128), primary_key=True),
            sa.Column("view_name", sa.String(128), primary_key=True),
            sa.Column("user_id", sa.String(64), primary_key=True),
            sa.Column("target", sa.String(1024)),
            sa.Column("target_type", sa.String(64)),
            sa.Column("obsolete", sa.SmallInteger()),  # some databases have problems with bool
        )

    def setup(self):
        super().setup()

        if self.metadata_store:
            # super().setup() already called self.metadata_store.setup() and
            # self.metadata_store.sql_metadata.create_all() incl. sync_views_table

            self.sql_metadata_local.create_all(self.engine, checkfirst=True)
            with self.engine_connect() as conn:
                self.user_id = conn.execute(sa.select(self.user_id_table.c.user_id)).scalar_one_or_none()
                if self.user_id is None:
                    # create a new UUID if it does not exist, yet
                    self.user_id = uuid.uuid4().hex
                    conn.execute(sa.insert(self.user_id_table).values(user_id=self.user_id))

    def get_storage_options(self, kind: Literal["polars", "fsspec"], protocol) -> dict[str, Any] | None:
        if protocol != "s3" or (self.s3_endpoint_url is None and self.s3_region is None):
            return None
        if kind == "polars":
            return {
                k: v
                for k, v in dict(
                    aws_endpoint_url=self.s3_endpoint_url,
                    aws_region=self.s3_region,
                    addressing_style=self.s3_url_style,
                ).items()
                if v is not None
            }
        elif kind == "fsspec":
            return {
                "client_kwargs": {
                    "endpoint_url": self.s3_endpoint_url,
                    "region_name": self.s3_region,
                },
                "config_kwargs": {"s3": {"addressing_style": self.s3_url_style}},
            }

    def _create_engine(self):
        engine = super()._create_engine()

        def execute(con, statement):
            """
            Execute a SQL statement and return the result.
            """
            if sa.__version__ >= "2.0.0":
                return con.execute(sa.text(statement))
            else:
                return con.execute(statement)

        with engine.connect() as con:
            if self.parquet_base_path.protocol == "s3":
                # Unfortunately it is hard to configure S3 URL endpoint consistently for fsspec, duckdb, and polars.
                # Thus, we take it as parameter to ParquetTableStore and pass it to fsspec. Fsspec in turn
                # is passed on to duckdb and polars. This is only needed for non-standard S3 endpoints like minio.
                from fsspec.config import conf  # fsspec’s global config

                if "s3" not in conf and self.s3_endpoint_url is not None:
                    conf.setdefault("s3", {})
                    conf["s3"].update(
                        {
                            "client_kwargs": {
                                "endpoint_url": self.s3_endpoint_url,
                            },
                            # ─── botocore.config.Config parameters go here ────
                            "config_kwargs": {
                                "connect_timeout": 3,
                                "read_timeout": 5,
                                "retries": {"max_attempts": 2, "mode": "standard"},
                            },
                        }
                    )
                    if self.s3_region:
                        conf["s3"]["client_kwargs"]["region_name"] = self.s3_region
                    if self.s3_url_style:
                        # MinIO needs path-style
                        conf["s3"]["config_kwargs"]["s3"] = dict(addressing_style=self.s3_url_style)

            if sa.__version__ >= "2.0.0":
                con.commit()

        # This is needed for gcsfs. For s3fs, it would also be possible to
        # go with httpfs (https://duckdb.org/docs/stable/core_extensions/httpfs/s3api.html).
        fs = fsspec.filesystem(self.parquet_base_path.protocol)
        engine.raw_connection().register_filesystem(fs)

        return engine

    def metadata_sync_views(self, schema_name: str):
        """
        Sync views between different users via metadata_store.

        This function interacts with metadata_track_* functions to get input in the metadata table sync_views.

        The per-user .duckdb file stores a sync_views_user_id table:
        - user_id  # UUID identifying the user taking part in the synchronization

        The sync_views table holds the following information:
        - schema [PK]
        - view_name [PK]
        - user_id [PK]
        - target  # parquet_file or target_schema
        - target_type  # "parquet": target is a parquet_file, "schema": target is a redirect schema name
        - obsolete  # if true (obsolete != 0), the parquet file is not in the correct place any more and
                    # the view must be removed

        Logic:
        - any schema reset will remove parquet files, delete all entries for schema
        - any view creation will insert or update the respective entry for the respective user_id; it will also update
        all entries by other user_ids to obsolete=True;
        - any view deletion will remove the respective entry for this user_id, and update all entries for other user_id
        values to obsolete=True
        - metadata_sync_views() will delete all views in the current schema if for the user_id, there is no entry for
        this schema. If there is already an entry for this user_id, it will delete all views in the current schema
          which are marked obsolete=True for this user_id, and it will create all views which are obsolete=False and
          from another user.
        """
        if self.metadata_store:
            with self.metadata_store.engine_connect() as meta_conn:
                tbl = self.sync_views_table
                match_user_id = (tbl.c.schema == schema_name) & (tbl.c.user_id == self.user_id)
                obsolete_views = meta_conn.execute(
                    sa.select(tbl.c.view_name).where(match_user_id & (tbl.c.obsolete != 0))
                ).fetchall()
                if len(obsolete_views) > 0:
                    self.logger.info(
                        "Removing obsolete views due to sync with metadata_store",
                        schema=schema_name,
                        views=[v[0] for v in obsolete_views],
                    )
                for view in obsolete_views:
                    try:
                        meta_conn.execute(DropView(view[0], schema_name, if_exists=False))
                    except sa.exc.OperationalError:
                        self.log.error(
                            f"Drop view failed while reconciling views with metadata_store: {schema_name}.{view[0]}"
                        )
                deleted = meta_conn.execute(tbl.delete().where(match_user_id & (tbl.c.obsolete != 0))).rowcount
                if deleted > 0:
                    self.logger.info(
                        "Deleted obsolete view entries in metadata_store",
                        schema=schema_name,
                        user_id=self.user_id,
                        affected=deleted,
                    )
                if len(obsolete_views) == 0:
                    user_id_cnt = meta_conn.execute(
                        sa.select(sa.func.count(sa.text("*"))).where(match_user_id)
                    ).fetchall()[0][0]
                    if user_id_cnt == 0:
                        # this user_id has no entry for this schema -> remove all views in this schema
                        with self.engine_connect() as conn:
                            existing_views = conn.execute(
                                sa.text(f"FROM duckdb_views() SELECT view_name WHERE schema_name='{schema_name}'")
                            ).fetchall()
                            if len(existing_views) > 0:
                                self.logger.info(
                                    "Removing all views due to sync with metadata_store",
                                    schema=schema_name,
                                    views=[v[0] for v in existing_views],
                                )
                            for view in existing_views:
                                try:
                                    conn.execute(DropView(view[0], schema_name, if_exists=False))
                                except sa.exc.OperationalError:
                                    self.log.error(
                                        "Drop view failed while reconciling views with metadata_store: "
                                        f"{schema_name}.{view[0]}"
                                    )

                query = sa.select(tbl.c.view_name, tbl.c.target, tbl.c.target_type).where(
                    (tbl.c.schema == schema_name) & (tbl.c.user_id != self.user_id) & (tbl.c.obsolete == 0)
                )
                create_views = meta_conn.execute(query).fetchall()
                if len(create_views) > 0:
                    self.logger.info(
                        "Creating views due to sync with metadata_store",
                        schema=schema_name,
                        views=[v[0] for v in create_views],
                    )
                with self.engine_connect() as conn:
                    del meta_conn  # prevent typo errors
                    for view, target, target_type in create_views:
                        try:
                            conn.execute(DropView(view, schema_name, if_exists=True))
                            schema = Schema(schema_name, prefix="", suffix="")
                            if target_type == "parquet":
                                conn.execute(CreateViewAsSelect(view, schema, self._read_parquet_query(target)))
                            elif target_type == "schema":
                                conn.execute(
                                    CreateViewAsSelect(view, schema, sa.text(f"SELECT * FROM {target}.{view}"))
                                )
                            else:
                                meta_engine = self.metadata_schema.engine
                                self.log.error(
                                    f"Unknown target_type in sync_views table: {target_type}\n"
                                    f"{query.compile(meta_engine, compile_kwargs={'literal_binds': True})}"
                                )
                        except sa.exc.OperationalError:
                            self.log.error(
                                "Create view failed while reconciling views with metadata_store: "
                                f"{schema_name}.{view} -> {target} ({target_type})"
                            )

    def metadata_track_view_flush(self, schema_name: str):
        """
        Flush all entries for schema in metadata_store.

        see metadata_sync_views() for details.
        """
        if self.metadata_store:
            with self.metadata_store.engine_connect() as meta_conn:
                tbl = self.sync_views_table
                deleted = meta_conn.execute(tbl.delete().where(tbl.c.schema == schema_name)).rowcount
                if deleted > 0:
                    self.logger.info(
                        "Flushed all view entries in metadata_store for schema", schema=schema_name, deleted=deleted
                    )

    def metadata_track_view(
        self, table_name: str, schema_name: str, target: str, target_type: Literal["parquet", "schema"]
    ):
        """
        Track creation of a view in metadata_store.

        see metadata_sync_views() for details.
        """
        if self.metadata_store:
            with self.metadata_store.engine_connect() as meta_conn:
                tbl = self.sync_views_table
                view_match = (tbl.c.schema == schema_name) & (tbl.c.view_name == table_name)
                # mark all other user views as obsolete
                updated = meta_conn.execute(tbl.update().where(view_match).values(obsolete=1)).rowcount
                if updated > 0:
                    self.logger.info(
                        "Marked views by other users as obsolete in metadata_store",
                        schema=schema_name,
                        view=table_name,
                        affected=updated,
                    )
                # drop obsolete own match:
                deleted = meta_conn.execute(tbl.delete().where(view_match & (tbl.c.user_id == self.user_id))).rowcount
                if deleted > 0:
                    self.logger.info(
                        "Deleted obsolete own view entries in metadata_store",
                        schema=schema_name,
                        view=table_name,
                        user_id=self.user_id,
                        deleted=deleted,
                    )
                # insert view
                meta_conn.execute(
                    sa.insert(tbl).values(
                        schema=schema_name,
                        view_name=table_name,
                        user_id=self.user_id,
                        target=target,
                        target_type=target_type,
                        obsolete=0,
                    )
                )
                self.logger.info(
                    "Inserted view entry in metadata_store",
                    schema=schema_name,
                    view=table_name,
                    target=target,
                    target_type=target_type,
                )

    def metadata_track_view_drop(self, table_name: str, schema_name: str):
        """
        Track deletion of a view in metadata_store.

        see metadata_sync_views() for details.
        """
        if self.metadata_store:
            with self.metadata_store.engine_connect() as meta_conn:
                tbl = self.sync_views_table
                view_match = (tbl.c.schema == schema_name) & (tbl.c.view_name == table_name)
                deleted = meta_conn.execute(tbl.delete().where(view_match & (tbl.c.user_id == self.user_id))).rowcount
                if deleted > 0:
                    self.logger.info(
                        "Deleted view entry in metadata_store", schema=schema_name, view=table_name, deleted=deleted
                    )
                # mark all other user views as obsolete
                updated = meta_conn.execute(tbl.update().where(view_match).values(obsolete=1)).rowcount
                if updated > 0:
                    self.logger.info(
                        "Marked views by other users as obsolete in metadata_store",
                        schema=schema_name,
                        view=table_name,
                        affected=updated,
                    )

    def on_clear_schema(self, schema_name: str):
        # this callback is called by parent class in _commit_stage_read_views()
        self.metadata_track_view_flush(schema_name)

    def on_read_view_alias(self, table_name: str, from_schema: str, to_schema: str):
        # this callback is called by parent class in _commit_stage_read_views()
        self.metadata_track_view(table_name, to_schema, target=from_schema, target_type="schema")

    def init_stage(self, stage: Stage):
        # fetch existing tables from database before schema is deleted there
        old_transaction_name = self._get_read_view_transaction_name(stage.name)
        new_transaction_name = self._get_read_view_original_transaction_name(stage, old_transaction_name)

        # first synchronize views to match the actual state in metadata_store
        if self.metadata_store:
            # Typically the parent schema stage.name references old_transaction schema.
            # But we also need to sync the new transaction schema to avoid double deletion of parquet files.
            schemas = [old_transaction_name] if old_transaction_name != "" else []
            schemas = schemas + [self.get_schema(stage.name).get(), new_transaction_name]
            for schema in schemas:
                self.execute(CreateSchema(Schema(schema, prefix="", suffix=""), if_not_exists=True))
                self.metadata_sync_views(schema)

        with self.engine_connect() as conn:
            existing_tables = self.execute(
                f"FROM duckdb_views() SELECT view_name WHERE "
                f"schema_name='{new_transaction_name}' "
                f"and sql like '%%FROM read_parquet(%%'",
                conn=conn,
            ).fetchall()
        # This creates transaction schema and modifies stage.current_name.
        # It also removes all views that were previously in transaction schema.
        super().init_stage(stage)
        self.metadata_track_view_flush(new_transaction_name)
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
                # import traceback
                self.logger.info(
                    "Cleaning up parquet file in transaction schema",
                    file_path=file_path,
                    # stacktrace="\n"+"\n".join(traceback.format_stack()),
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
        self.metadata_track_view(table.name, dest_schema.get(), dest_file_path.as_uri(), "parquet")

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
                self.metadata_track_view(table.name, schema.get(), dest_file_path.as_uri(), "parquet")
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
        return self.parquet_base_path / self.instance_id / schema.get()

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
        try:
            cfg = ConfigContext.get()
        except LookupError:
            # schema-prefix/suffix not available if no ConfigContext active
            return self.get_parquet_schema_path(Schema(stage.current_name))
        store = cfg.store.table_store
        return self.get_parquet_schema_path(store.get_schema(stage.current_name))

    def get_table_path(
        self,
        table: Table,
        file_extension: str = ".parquet",
    ) -> UPath:
        schema_name = table.stage.current_name
        try:
            cfg = ConfigContext.get()
            store = cfg.store.table_store
            schema = store.get_schema(schema_name)
        except LookupError:
            # schema-prefix/suffix not available if no ConfigContext active
            schema = Schema(schema_name)
        return self.get_table_schema_path(table.name, schema, file_extension)

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
        view_schema = self.get_schema(table.stage.transaction_name)
        self.execute(
            DropView(
                table.name,
                view_schema,
                if_exists=True,
            )
        )
        self.metadata_track_view_drop(table.name, view_schema.get())

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
        self.metadata_track_view_drop(table.name, schema.get())
        # create view again
        self.execute(CreateViewAsSelect(to_name, schema, self._read_parquet_query(to_path)))
        self.metadata_track_view(to_name, schema.get(), to_path.as_uri(), "parquet")

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
        self.metadata_track_view(to_name, schema.get(), file_path.as_uri(), "parquet")
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
        self.metadata_track_view_drop(drop_name, schema.get())


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
        df.to_parquet(
            str(file_path), index=False, storage_options=store.get_storage_options("fsspec", file_path.protocol)
        )
        store.execute(CreateViewAsSelect(table.name, schema, store._read_parquet_query(file_path)))
        store.metadata_track_view(table.name, schema.get(), file_path.as_uri(), "parquet")

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type,
        limit: int | None = None,
    ):
        # view resolution in input table is implemented here using pyarrow

        path = store.get_table_path(table)
        import pyarrow.dataset as ds

        pyarrow_path, pyarrow_fs = cls.get_pyarrow_path(path, store)
        first_pyarrow_path = pyarrow_path

        if table.view:
            view = table.view
            assert view.assert_normalized
            if isinstance(view.src, Iterable):
                path = [store.get_table_path(tbl) for tbl in view.src]
                pyarrow_path = [cls.get_pyarrow_path(p, store)[0] for p in path]
                first_pyarrow_path = pyarrow_path[0]
            else:
                path = store.get_table_path(view.src)
                pyarrow_path, _ = cls.get_pyarrow_path(path, store)
                first_pyarrow_path = pyarrow_path

            ads = ds.dataset(pyarrow_path, filesystem=pyarrow_fs)
            if view.sort_by is not None:
                # Unfortunately this loads the full table with all columns in memory.
                # Calling PolarsTableHook might be more efficient if a dataset is large and many
                # columns are dropped in view.columns.
                sort_cols = view.sort_by if isinstance(view.sort_by, Iterable) else [view.sort_by]
                ads = ads.sort_by(
                    [
                        (sort_col.col, "descending" if sort_col.order == SortOrder.DESC else "ascending")
                        for sort_col in sort_cols
                    ]
                )
            import pyarrow.compute as pc

            ads = ads.scanner(columns={k: pc.field(v) for k, v in view.columns.items()} if view.columns else None)
            if view.limit is not None:
                ads = ads.head(view.limit)
            else:
                ads = ads.to_table()  # whole table (needed for to_pandas below)
            df = ads.to_pandas()
        else:
            if limit is not None:
                df = ds.dataset(pyarrow_path, filesystem=pyarrow_fs).scanner().head(limit).to_pandas()
            else:
                df = pd.read_parquet(str(path), storage_options=store.get_storage_options("fsspec", path.protocol))
        import pyarrow.parquet

        # attention: with categorical columns, it might be necessary to fuse dictionaries of all parquet files
        schema = pyarrow.parquet.read_schema(first_pyarrow_path, filesystem=pyarrow_fs)
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
        if table.name is not None:
            df.attrs["name"] = table.name
        return df

    @staticmethod
    def get_pyarrow_path(path: UPath, store: ParquetTableStore) -> tuple[str, fsspec.AbstractFileSystem]:
        if path.protocol == "s3" and (store.s3_endpoint_url or store.s3_region):
            # Setting endpoint URL is different for fsspec, pyarrow, duckdb, polars, and pandas.
            # ParquetTableStore allows setting endpoint_url in pipedag.yaml and copies it to fsspec and duckdb.
            # Here, we copy it from fsspec to pyarrow.
            pyarrow_path = path.path
            # unfortunately, force_virtual_addressing=store.s3_url_style=="path" does not work for
            # pyarrow.parquet.read_schema(pyarrow_path, filesystem=pyarrow_fs)
            import pyarrow.fs

            pyarrow_fs = pyarrow.fs.S3FileSystem(endpoint_override=store.s3_endpoint_url, region=store.s3_region)
        else:
            pyarrow_path = path
            pyarrow_fs = None
        return pyarrow_path, pyarrow_fs

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        return sql_hooks.PandasTableHook.auto_table(obj)

    @classmethod
    def get_computation_tracer(cls):
        return sql_hooks.PandasTableHook.ComputationTracer()


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
        if options := store.get_storage_options("polars", file_path.protocol):
            if pl.__version__ < "1.3.0":
                raise RuntimeError(
                    "Storing polars tables with custom storage options is not supported for polars < 1.3.0. "
                    f"Current version is {pl.__version__}: {options}"
                )
            # at some point polars supported UPath, but 1.33.1 does not
            df.write_parquet(str(file_path), storage_options=options)
        else:
            df.write_parquet(str(file_path))
        if store.allow_overwrite:
            store.execute(DropView(table.name, schema, if_exists=True))
        store.execute(CreateViewAsSelect(table.name, schema, store._read_parquet_query(file_path)))
        store.metadata_track_view(table.name, schema.get(), file_path.as_uri(), "parquet")

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
        if table.view:
            view = table.view
            if isinstance(view.src, Iterable):
                file_paths = [store.get_table_path(tbl) for tbl in view.src]
                protocol = file_paths[0].protocol
                file_paths = [str(path) for path in file_paths]
            else:
                file_paths = store.get_table_path(view.src)
                protocol = file_paths.protocol
                file_paths = str(file_paths)
            lf = pl.scan_parquet(
                file_paths, n_rows=limit, storage_options=store.get_storage_options("polars", protocol)
            )
            if view.sort_by is not None:
                sort_cols = [c.col for c in view.sort_by]
                sort_desc = [c.order == SortOrder.DESC for c in view.sort_by]
                sort_nulls_last = [c.nulls_first == False for c in view.sort_by]  # noqa: E712
                lf = lf.sort(sort_cols, descending=sort_desc, nulls_last=sort_nulls_last)
            if view.columns is not None:
                lf = lf.select(**view.columns)
            if view.limit:
                lf = lf.limit(view.limit)
            return lf.collect()
        else:
            file_path = store.get_table_path(table)
            df = pl.read_parquet(
                str(file_path), n_rows=limit, storage_options=store.get_storage_options("polars", file_path.protocol)
            )
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
        store.metadata_track_view(table_name, schema.get(), file_path.as_uri(), "parquet")
        return [
            CopySelectTo(
                file_path,
                "PARQUET",
                query,
            ),
            CreateViewAsSelect(table_name, schema, store._read_parquet_query(file_path)),
        ]

    @classmethod
    def get_view_query(cls, view: View, store: ParquetTableStore):
        """ParquetTableStore implements special version for src-only views."""
        if view.columns is None and view.sort_by is None and view.limit is None:
            if isinstance(view.src, Iterable):
                try:
                    from sqlalchemy.sql.base import ColumnCollection, ReadOnlyColumnCollection

                    possible = True
                except LookupError:
                    possible = False

                if possible:
                    src_tables = list(view.src)
                    files = [str(store.get_table_path(tbl)) for tbl in src_tables]
                    assert len(src_tables) > 0
                    tbl1 = SQLAlchemyTableHook.retrieve(
                        store, src_tables[0], src_tables[0].stage.current_name, sa.Table
                    )
                    cols = tbl1.c  # this might be oversimplified for categorical columns
                    file_refs = "['" + "','".join(files) + "']"
                    base_from = sa.select(sa.text("*")).select_from(sa.text(f"read_parquet({file_refs})")).alias("sub")

                    # reconstruct columns which were lost by select("*")
                    def bind(c: sa.Column, expr):
                        col = sa.Column(c.name, c.type)
                        col.table = expr
                        return col

                    base_from.c = ReadOnlyColumnCollection(
                        ColumnCollection([(c.name, bind(c, base_from)) for c in cols])
                    )
                    return base_from
        return sql_hooks.get_view_query(view, store)
