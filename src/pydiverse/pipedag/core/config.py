from __future__ import annotations

import copy
import getpass
import itertools
import os
import re
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
import structlog
import yaml
from box import Box

from pydiverse.pipedag.util.deep_merge import deep_merge

if TYPE_CHECKING:
    from pydiverse.pipedag.context import ConfigContext


# noinspection PyPep8Naming
class cached_class_property:
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        if not hasattr(self, "cache"):
            self.cache = self.func(cls)
        return self.cache


class PipedagConfig:
    """
    This class represents a :doc:`pipedag config file </reference/config>`.

    :param path: Path to the config yaml file to load or a dictionary containing the
        raw config as it would be loaded from yaml file.

    Attributes
    ----------
    default : PipedagConfig
        The default config file.

        If the environment variable :envvar:`PIPEDAG_CONFIG` is set, then this file
        will be used as the config file. Otherwise, pipedag searches for a file called
        ``pipedag.yaml`` or ``pipedag.yml`` in:

        * The current working directory
        * Any parent directories of the working directory
        * The user folder
    """

    default: PipedagConfig

    def __init__(self, path: str | Path | dict[str, Any]):
        self.path = None
        if isinstance(path, dict):
            self.raw_config = path
        else:
            if isinstance(path, str):
                path = Path(path)
            self.path = path
            with open(path) as f:
                self.raw_config = yaml.safe_load(f)

        self.config_dict = self.__parse_config(self.raw_config)

    @cached_class_property
    def default(cls):
        config_path = find_config()
        return PipedagConfig(config_path)

    @property
    def name(self) -> str:
        return self.config_dict.get("name")

    def __parse_config(self, raw_config: dict[str:Any]):
        config = copy.deepcopy(raw_config)

        # Expand all references
        for instance_dict in _get(config, "instances", default={}).values():
            self.__expand_references(instance_dict)

        for flow_dict in _get(config, "flows", default={}).values():
            self.__expand_references(flow_dict)
            for instance_dict in _get(flow_dict, "instances", default={}).values():
                self.__expand_references(instance_dict)

        return config

    def __expand_references(self, config):
        references = [
            (None, "technical_setup", "technical_setups"),
            ("table_store", "table_store_connection", "table_store_connections"),
            ("blob_store", "blob_store_connection", "blob_store_connections"),
        ]

        for expand_path, ref_name_path, ref_src_path in references:
            ref_name = _get(config, expand_path, ref_name_path, default=None)
            if ref_name is None:
                continue

            base_dict = _get(config, expand_path)
            merged_dict = deep_merge(
                base_dict, copy.deepcopy(_get(self.raw_config, ref_src_path, ref_name))
            )
            base_dict.update(merged_dict)
            _pop(base_dict, ref_name_path)

        return config

    def get(
        self,
        instance: str | None = None,
        flow: str | None = None,
        per_user: bool = False,
    ) -> ConfigContext:
        """
        Constructs a :py:class:`ConfigContext`.
        For more details how the specific ConfigContext instance is constructed,
        check out the :ref:`specifying instances and flows
        <reference/config:Specifying instances and flows>` section.

        :param instance: Name of the instance.
            If no value is provided the ``__any__`` instance gets used.
        :param flow: Name of the flow.
            If no value is provided the ``__any__`` flow gets used.
        :param per_user:
            Whether to customize the instance id for each user according to
            :ref:`per_user_template`.
        """

        # TODO: Check that this function only gets called in the main interpreter.
        #       Otherwise certain environment variables might get expanded incorrectly.
        from pydiverse.pipedag.context import ConfigContext

        config = self.__get_merged_config_dict(
            instance=instance,
            flow=flow,
            default={
                "fail_fast": False,
                "network_interface": "127.0.0.1",
                "per_user_template": "{id}_{username}",
                "strict_result_get_locking": True,
                "ignore_task_version": False,
                "stage_commit_technique": "SCHEMA_SWAP",
                "auto_table": [],
                "auto_blob": [],
                "attrs": {},
            },
        ).copy()

        # check enums
        # Alternative: could be a feature of __get_merged_config_dict
        # in case default value is set to Enum
        from pydiverse.pipedag.context.context import StageCommitTechnique

        config["stage_commit_technique"] = (
            config["stage_commit_technique"].strip().upper()
        )
        if not hasattr(StageCommitTechnique, config["stage_commit_technique"]):
            raise ValueError(
                "Found unknown setting stage_commit_technique:"
                f" '{config['stage_commit_technique']}'; Expected one of:"
                f" {', '.join([v.name for v in StageCommitTechnique])}"
            )
        stage_commit_technique = getattr(
            StageCommitTechnique, config["stage_commit_technique"]
        )

        # TODO: Delegate selecting where variables can be expanded to the
        #  corresponding classes.
        #    eg. SQLTableStore._expand_env_vars = ["url", "url_attrs_file"]
        #    eg. SQLTableStore._expand_vars = ["url", "schema_prefix", "schema_suffix"]

        # First expand all environment variables
        self.__expand_environment_variables(inout_config=config)

        # Intermediate variable processing
        if per_user:
            template = config["per_user_template"]
            config["instance_id"] = expand_variables(
                template,
                {
                    "username": getpass.getuser(),
                    "id": config["instance_id"],
                },
            )

        # Handle url_attrs_file
        url_attrs_file = _pop(
            config, "table_store", "args", "url_attrs_file", default=None
        )
        if url_attrs_file is not None:
            with open(url_attrs_file, encoding="utf-8") as fh:
                url_attrs = yaml.safe_load(fh)

            url = _get(config, "table_store", "args", "url")
            url = expand_variables(url, url_attrs, skip_missing=True)
            _set(config, url, "table_store", "args", "url")

        # Finally, expand all normal variables
        config = self.__expand_variables(config)

        # Construct final ConfigContext
        config_context = ConfigContext(
            config_dict=config,
            pipedag_name=self.name,
            flow_name=flow,
            strict_result_get_locking=config["strict_result_get_locking"],
            ignore_task_version=config["ignore_task_version"],
            instance_name=instance,
            instance_id=config["instance_id"],
            stage_commit_technique=stage_commit_technique,
            fail_fast=config["fail_fast"],
            network_interface=config["network_interface"],
            kroki_url=config.get("kroki_url"),
            attrs=Box(config["attrs"], frozen_box=True),
            table_hook_args=Box(
                config["table_store"].get("hook_args", {}), frozen_box=True
            ),
        )

        if "PYDIVERSE_PIPEDAG_PYTEST" not in os.environ:
            # If we're running test cases, this can be skipped to improve performance
            try:
                # Make sure @cached_property store is set up and loaded
                # and throw config errors early.
                with config_context:
                    _ = config_context.store
                    _ = config_context.auto_table
                    _ = config_context.auto_blob

                    config_context.create_orchestration_engine().dispose()
                    config_context.create_lock_manager().dispose()
            except Exception as e:
                raise RuntimeError(
                    "Error while creating backend objects from pipedag config "
                    f"(instance={instance}, flow={flow}): {self.path}"
                ) from e

        return config_context

    def __get_merged_config_dict(self, instance, flow, default=None):
        search_paths = [
            ("instances", "__any__"),
            ("instances", instance),
            ("flows", "__any__"),
            ("flows", "__any__", "instances", instance),
            ("flows", flow),
            ("flows", flow, "instances", instance),
        ]

        search_paths = [path for path in search_paths if None not in path]
        dicts = [_get(self.config_dict, path, default=None) for path in search_paths]

        # Check for strict instance lookup
        # If instance is specified, make sure that a corresponding section can be found
        strict_instance_lookup = self.config_dict.get("strict_instance_lookup", True)
        if strict_instance_lookup and instance is not None:
            found_instance = False
            for path, d in zip(search_paths, dicts):
                found_instance |= (
                    "instances" in path and instance in path and d is not None
                )

            if not found_instance:
                raise AttributeError(
                    "Strict instance lookup failed: Couldn't find instance"
                    f" '{instance}' in pipedag config."
                )

        # Merge
        merged = default or {}
        for d in dicts:
            if d is not None:
                merged = deep_merge(merged, d)

        return merged

    @staticmethod
    def __expand_environment_variables(*, inout_config):
        locations = [
            ("table_store", "args", "url"),
            ("table_store", "args", "url_attrs_file"),
        ]

        for location in locations:
            value: str = _get(inout_config, location, default=None)
            if value is None:
                continue

            value = expand_environment_variables(value)
            _set(inout_config, value, location)

    def __expand_variables(self, config) -> dict[str, Any]:
        out_config = copy.deepcopy(config)
        locations = [
            ("table_store", "args", "url"),
            ("table_store", "args", "schema_prefix"),
            ("table_store", "args", "schema_suffix"),
        ]

        # TODO: Decide on a list of available variables
        variables = {
            "username": getpass.getuser(),
            "instance_id": config.get("instance_id"),
            "name": self.name,
        }

        for key, val in list(variables.items()):
            if val is None:
                variables.pop(key)

        for location in locations:
            value: str = _get(config, location, default=None)
            if value is None:
                continue

            value = expand_variables(value, variables)
            _set(out_config, value, location)
        return out_config


def create_basic_pipedag_config(
    engine_url: str,
    blob_directory: str | Path | None = None,
    name: str = "pipeline",
    instance_id: str = "pipeline",
    network_interface="127.0.0.1",
    auto_table=(
        "pandas.DataFrame",
        "sqlalchemy.sql.expression.TextClause",
        "sqlalchemy.sql.expression.Selectable",
    ),
    fail_fast=True,
    disable_stage_locking=False,
    file_locking_directory: str | Path | None = None,
) -> PipedagConfig:
    """
    Get code-based pipedag config as easy as providing a SQLAlchemy URL.

    This is an alternative to ``pipedag.yaml`` files which are typically loaded with
    ``PipedagConfig.default`` or ``PipedagConfig("path/pipedag.yaml")``.
    Feel free to extract ``create_basic_pipedag_config(url).raw_config``, modify it
    and to create a new ``PipedagConfig(raw_config)``.

    For more mature projects that configure multiple pipeline instances, it is
    recommended to stay with ``pipedag.yaml`` files though.

    Example::

        cfg = create_basic_pipedag_config(
            "duckdb:////tmp/pipedag/{instance_id}/db.duckdb",
            disable_stage_locking=True
        ).get("default")
        with cfg:
            with Flow() as flow:
                with Stage("step00"):
                    x = some_task()
            flow.run()

    Examples for engine_urls::

      "postgresql://{$DB_USER}:{$DB_PASSWORD}@{$DB_HOST}:{$DB_PORT}/{instance_id}"
      "postgresql://sa:Pydiverse23@127.0.0.1:6543/{instance_id}"
      "mssql+pyodbc://sa:PydiQuant27@127.0.0.1:1433/{instance_id}?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no"
      "db2+ibm_db://db2inst1:password@localhost:50000/testdb"
      "duckdb:////tmp/pipedag/{instance_id}/db.duckdb"

    :param engine_url:
        SQLAlchemy engine url. It may include references to ENVIRONMENT VARIABLES
        like ``{$DB_PASSWORD}``. Furthermore, ``{name}`` is replaced with pipeline
        name and ``{instance_id}`` is replaced with the instance_id (see below).
    :param blob_directory:
        Default None;
        Directory where blobs are stored. In case of None, no task in the pipeline
        is allowed to return a ``Blob()`` object.
    :param name: pipeline name
    :param instance_id: ``instance_id`` used as placeholder in parameters ``engine_url``
        and ``blob_directory``. It is more relevant for non-basic configuration with
        multiple pipeline instances.
    :param network_interface:
        Used for shared state communication while executing flow. It is possible
        to perform multi-node parallel execution with the DaskEngine. You probably
        don't need to worry and just leave it at ``127.0.0.1`` for localhost
    :param auto_table:
        You might be just happy with the default to convert pandas DataFrames
        and SQLAlchemy clauses to Tables when returning them in tasks. If you use
        pipedag.Table() objects for manually choosing table names, this is not
        relevant.
    :param fail_fast:
        Default True;
        Aborts execution of pipeline on first error. This is often easier to
        read stacktraces. If you want to see all errors a more complex DAG, please
        set to False.
    :param disable_stage_locking:
        Default False;
        When True, choose NoLockManager as opposed to DatabaseLockManager. In this
        case, two parallel runs of the pipeline can easily corrupt schema swapping.
        Unfortunately, we don't support database locking for duckdb engine_urls.
        So you must either use ``disable_stage_locking=True`` or set
        ``file_locking_directory``.
    :param file_locking_directory:
        Default None;
        If string or Path is provided, chose file based locking in the given directory.
        We use database locking by default (``file_locking_directory=None`` and
        ``disable_stage_locking=False``).
        However, we only support this for MSSQL, Postgres, and DB2 dialects so far.
    :return:
        PipedagConfig object
    """

    # test engine:
    engine = sa.create_engine(engine_url)
    dialect = engine.dialect.name
    engine.dispose()

    if dialect not in ["mssql", "postgresql", "ibm_db_sa", "duckdb"]:
        logger = structlog.get_logger(
            logger_name=__name__, function="create_basic_pipedag_config"
        )
        logger.info("")

    if (
        dialect not in ["mssql", "postgresql", "ibm_db_sa"]
        and not disable_stage_locking
        and file_locking_directory is None
    ):
        raise AttributeError(
            f"We don't support Database Locking for database dialect {dialect}. "
            "Please consider setting disable_stage_locking=True (i.e. for local DuckDB)"
            " or file_locking_directory for team shared database."
        )

    if dialect in ["duckdb", "ibm_db_sa"]:
        stage_commit_technique = "read_views"
    else:
        stage_commit_technique = "schema_swap"
    table_store = {
        "class": "pydiverse.pipedag.backend.table.SQLTableStore",
        "args": dict(url=engine_url, print_materialize=True, print_sql=True),
    }
    if blob_directory is None:
        blob_store = {"class": "pydiverse.pipedag.backend.blob.NoBlobStore"}
    else:
        blob_store = {
            "class": "pydiverse.pipedag.backend.blob.FileBlobStore",
            "args": dict(base_path=str(blob_directory)),
        }
    if disable_stage_locking:
        lock_manager = {"class": "pydiverse.pipedag.backend.lock.NoLockManager"}
    elif file_locking_directory is not None:
        lock_manager = {
            "class": "pydiverse.pipedag.backend.lock.FileLockManager",
            "args": dict(base_path=str(file_locking_directory)),
        }
    else:
        lock_manager = {"class": "pydiverse.pipedag.backend.lock.DatabaseLockManager"}
    orchestration = {"class": "pydiverse.pipedag.engine.SequentialEngine"}
    raw_config = dict(
        name=name,
        strict_instance_lookup=False,
        instances=dict(
            __any__=dict(
                network_interface=network_interface,
                auto_table=list(auto_table),
                fail_fast=fail_fast,
                instance_id=instance_id,
                stage_commit_technique=stage_commit_technique,
                table_store=table_store,
                blob_store=blob_store,
                lock_manager=lock_manager,
                orchestration=orchestration,
            )
        ),
    )
    return PipedagConfig(raw_config)


def find_config(
    name: str = "pipedag",
    search_paths: Iterable[str | Path] = None,
) -> str:
    """Searches for a pipedag config file

    The following paths get checked first.

    - The path specified in the "PIPEDAG_CONFIG" environment variable

    Else it searches in the following locations:

    - Current working directory
    - All parent directories
    - The user folder

    :param name: The name of the config file
    :param search_paths: The directories in which to search for the config file
    :return: The path of the file.
    :raises FileNotFoundError: if no config file could be found.
    """

    extensions = [".yaml", ".yml"]

    if search_paths is None:
        # Check PIPEDAG_CONFIG path
        if path := os.environ.get("PIPEDAG_CONFIG", None):
            path = Path(path).resolve().expanduser()
            if path.is_file():
                return str(path)

            for extension in extensions:
                path = path / (name + extension)
                if path.is_file():
                    return str(path)

        # Else, search in these default directories
        search_paths = [
            Path.cwd(),
            *Path.cwd().resolve().parents,
            Path("~").expanduser(),
        ]
    else:
        search_paths = [Path(path) for path in search_paths]

    file_names = [name + extension for extension in extensions]
    for path, file_name in itertools.product(search_paths, file_names):
        config_path: Path = (path / file_name).resolve()
        if config_path.is_file():
            return str(config_path)

    raise FileNotFoundError("No config file found")


def expand_environment_variables(string: str) -> str:
    """
    Expands all occurrences of the form ``{$ENV_VAR}`` with the environment variable
    named ``ENV_VAR``.
    """

    def env_var_sub(match: re.Match):
        name = match.group()[2:-1]
        if name not in os.environ:
            raise AttributeError(
                f"Could not find environment variable '{name}' "
                f"referenced in '{string}'."
            )
        return os.environ[name]

    return re.sub(r"\{\$[a-zA-Z_]+[a-zA-Z0-9_]*\}", env_var_sub, string)


def expand_variables(
    string: str, variables: dict[str:str], skip_missing: bool = False
) -> str:
    """
    Expands all occurrences of the form {var_name} with the variable
    named ``var_name``.
    """

    def var_sub(match: re.Match):
        name = match.group()[1:-1]
        if name not in variables:
            if skip_missing:
                return match.group()
            raise AttributeError(
                f"Could not find variable '{name}' referenced in '{string}'."
            )
        return str(variables[name])

    return re.sub(r"\{[a-zA-Z_]+[a-zA-Z0-9_]*\}", var_sub, string)


# Nested Dictionary Utilities


def _flatten(items):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from _flatten(x)
        else:
            yield x


_nil = object()


def _get(d, *path, default=_nil):
    path = [x for x in _flatten(path) if x is not None]
    try:
        for p in path:
            if p is not None:
                d = d[p]
    except (IndexError, KeyError) as e:
        if default is _nil:
            raise KeyError(f"Invalid path: {' > '.join(path)}") from e
        return default

    return d


def _set(d, value, *path):
    path = [x for x in _flatten(path) if x is not None]
    for p in path[:-1]:
        d = d[p]
    d[path[-1]] = value


def _pop(d, *path, default=_nil) -> Any:
    try:
        path = [x for x in _flatten(path) if x is not None]
        for p in path[:-1]:
            d = d[p]
        return d.pop(path[-1])
    except KeyError as e:
        if default == _nil:
            raise e
        return default
