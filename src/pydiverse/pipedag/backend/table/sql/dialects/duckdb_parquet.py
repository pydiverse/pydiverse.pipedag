from __future__ import annotations

from typing import Any

from pydiverse.pipedag.backend.table.sql.dialects import DuckDBTableStore
from pydiverse.pipedag.backend.table.sql.sql import DISABLE_DIALECT_REGISTRATION


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
        prefix. Examples are '/tmp/pipedag/parquet/' or 'S3

    """

    _dialect_name = DISABLE_DIALECT_REGISTRATION

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        config = config.copy()
        engine_url = config.pop("url")
        return cls(engine_url, **config)

    def __init__(
        self,
        engine_url: str,
        parquet_base_path: str,
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
        )
        self.parquet_base_path = parquet_base_path

        self.logger.info(
            "Initialized Parquet Table Store",
            parquet_base_path=self.parquet_base_path,
            engine_url=self.engine_url.render_as_string(hide_password=True),
            schema_prefix=self.schema_prefix,
            schema_suffix=self.schema_suffix,
        )
