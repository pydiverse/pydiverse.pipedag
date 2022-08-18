from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from pydiverse.pipedag.util.import_ import import_object

if TYPE_CHECKING:
    from pydiverse.pipedag.context import ConfigContext


def find_config(name="pipedag.toml"):
    # TODO: Allow specifying config using environment variable
    dirs_to_check = [
        Path("."),
        *Path(".").resolve().parents,
        Path("~").expanduser(),
    ]

    for path in dirs_to_check:
        config_path = (path / name).resolve()
        if config_path.is_file():
            return config_path

    raise FileNotFoundError("No config file found")


def load_config(path: str) -> ConfigContext:
    from pydiverse.pipedag.context import ConfigContext

    path = os.path.expanduser(path)
    path = os.path.normpath(path)

    with open(path, "rb") as f:
        config_dict = tomllib.load(f)

    # Parse Config File

    name = config_dict.get("name", None)

    auto_table = tuple(map(import_object, config_dict.get("auto_table", ())))
    auto_blob = tuple(map(import_object, config_dict.get("auto_blob", ())))

    return ConfigContext(
        config_dict=config_dict.copy(),
        name=name,
        auto_table=auto_table,
        auto_blob=auto_blob,
    )


def load_instance(config_dict: dict):
    """Instantiates an instance of an object given

    The import path (module.Class) should be specified as the "class" value
    of the dict. The rest of the dict get used as the instance config.

    If the class defines a `_init_conf_` function, it gets called using the
    config valuesdef , otherwise they just get passed to the class initializer.

    >>> # module.Class(argument="value")
    >>> load_instance({
    >>>     "class": "module.Class",
    >>>     "argument": "value",
    >>> })

    """

    config_dict = config_dict.copy()
    cls = import_object(config_dict.pop("class"))

    try:
        init_conf = getattr(cls, "_init_conf_")
        return init_conf(config_dict)
    except AttributeError:
        return cls(**config_dict)
