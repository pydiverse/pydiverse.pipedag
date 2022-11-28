from __future__ import annotations

import copy
import os
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable

import yaml

from pydiverse.pipedag.util.deep_merge import deep_merge
from pydiverse.pipedag.util.import_ import import_object


def find_config(name: str = "pipedag", extensions: Iterable[str] = (".yaml", ".yml")):
    """
    Searches for a pipedag instance configuration file.

    The following paths get checked first.

    - The path specified in the "PIPEDAG_CONFIG" environment variable

    Else it searches in the following locations:

    - Current working directory
    - All parent directories
    - The user folder

    :param name: The name of the config file
    :param extensions: Iterable of extensions which are looked for
    :return: The path of the file.
    :raises FileNotFoundError: if no config file could be found.
    """

    # Check PIPEDAG_CONFIG path
    if path := os.environ.get("PIPEDAG_CONFIG", None):
        path = Path(path).resolve().expanduser()
        if path.is_file():
            return path

        for extension in extensions:
            path = path / (name + extension)
            if path.is_file():
                return path

    # Search in other directories
    dirs_to_check = [
        Path.cwd(),
        *Path.cwd().resolve().parents,
        Path("~").expanduser(),
    ]

    for path in dirs_to_check:
        for extension in extensions:
            config_path = (path / (name + extension)).resolve()
            if config_path.is_file():
                return config_path

    raise FileNotFoundError(
        f"No config file found with name={name}, extensions={extensions}"
    )


def load_config(path: str) -> InstanceConfig:
    path = os.path.expanduser(path)
    path = os.path.normpath(path)

    with open(path, encoding="utf-8") as fh:
        instance_config_dict = yaml.safe_load(fh)

    # TODO: do some schema validation (i.e. with JSON Schema)
    assert "instances" in instance_config_dict
    assert "database_connections" in instance_config_dict
    return InstanceConfig(instance_config_dict, path)


def load_object(config_dict: dict, instance_config: InstanceConfig):
    """Instantiates an instance of an object given

    The import path (module.Class) should be specified as the "class" value
    of the dict. The rest of the dict get used as the instance config.

    If the class defines a `_init_conf_` function, it gets called using the
    config valuesdef , otherwise they just get passed to the class initializer.

    >>> # module.Class(argument="value")
    >>> load_object({
    >>>     "class": "module.Class",
    >>>     "argument": "value",
    >>> })

    """

    config_dict = config_dict.copy()
    cls = import_object(config_dict.pop("class"))

    try:
        init_conf = getattr(cls, "_init_conf_")
        return init_conf(config_dict, instance_config=instance_config)
    except AttributeError:
        return cls(**config_dict)


def get_config():
    return load_config(find_config())


class InstanceConfig:
    def __init__(self, instance_config_dict: dict[str, Any], config_file: str):
        self.instance_config_dict = instance_config_dict
        # TODO: provide good error messages about configuration with line number
        self.config_file = config_file

    def get_flow_name(self):
        return self.instance_config_dict.get("name")

    def get(self, instance="default", per_user=False, ignore_fresh_input=False):
        config_dict = self.instance_config_dict["instances"].get("default", {})
        if instance != "default":
            config_dict = deep_merge(
                config_dict, self.instance_config_dict["instances"][instance]
            )

        # This could also be replaced by using yaml anchor syntax, but forcing people to use indirection
        # for database_connections is also not that bad
        config_dict = deep_merge(
            config_dict,
            self.expand_database_connection(
                config_dict["table_store"],
                config_dict["table_store"]["database_connection"],
            ),
        )

        # Parse Config
        name = self.instance_config_dict.get("name", None)
        interface = config_dict.get("network_interface", "127.0.0.1")

        auto_table = tuple(map(import_object, config_dict.get("auto_table", ())))
        auto_blob = tuple(map(import_object, config_dict.get("auto_blob", ())))
        fail_fast = config_dict.get("fail_fast", False)
        flow_attributes = config_dict.get("flow_attributes", {})

        table_store = load_object(config_dict["table_store"], self)
        blob_store = load_object(config_dict["blob_store"], self)
        lock_manager = load_object(config_dict["lock_manager"], self)
        engine = load_object(config_dict["engine"], self)

        from pydiverse.pipedag.context import ConfigContext
        from pydiverse.pipedag.materialize.store import PipeDAGStore

        store = PipeDAGStore(
            table=table_store,
            blob=blob_store,
        )

        return ConfigContext(
            config_dict=config_dict.copy(),
            name=name,
            network_interface=interface,
            auto_table=auto_table,
            auto_blob=auto_blob,
            fail_fast=fail_fast,
            flow_attributes=flow_attributes,
            store=store,
            lock_manager=lock_manager,
            engine=engine,
        )

    def expand_database_connection(
        self, table_store: dict[str, Any], database_connection: str
    ):
        connections = self.instance_config_dict["database_connections"]
        # fill in table_store.engine attribute from database_connections and database attributes
        if database_connection not in connections:
            # TODO: provide good error messages about configuration with line number
            raise AttributeError(
                f"Could not find database_connections.{database_connection} in"
                f" {self.config_file}"
            )

        connection = copy.deepcopy(connections[database_connection])
        database = table_store.get("database", connection.get("database"))

        # replace database parameter in all values of connection
        if database is not None:
            for key, value in list(connection.items()):
                if isinstance(value, str) and "{database}" in value:
                    connection[key] = connection[key].format(database=database)
            # table_store.engine may be dictionary with table_store.engine.url
            if isinstance(connection["engine"], Mapping):
                for key, value in list(connection["engine"].items()):
                    if isinstance(value, str) and "{database}" in value:
                        connection["engine"][key] = connection[key].format(
                            database=database
                        )

        # table_store can still override connection attributes explicitly
        return dict(table_store=deep_merge(connection, table_store))
