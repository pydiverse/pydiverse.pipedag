from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any, Iterable

import structlog
import yaml

from pydiverse.pipedag.context import ConfigContext
from pydiverse.pipedag.util.deep_merge import deep_merge


def find_config(
    name: str | None = None,
    extensions: Iterable[str] | None = None,
    dirs_to_check: Iterable[str | Path] = None,
) -> str:
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
    :param dirs_to_check: Iterable of dictionaries to look for configuration file
    :return: The filename path of the pipedag config file.
    :raises FileNotFoundError: if no config file could be found.
    """

    if name is None:
        name = "pipedag"

    if extensions is None:
        extensions = [".yaml", ".yml"]

    if dirs_to_check is None:
        dirs_to_check = [
            Path.cwd(),
            *Path.cwd().resolve().parents,
            Path("~").expanduser(),
        ]

        # Check PIPEDAG_CONFIG path
        if path := os.environ.get("PIPEDAG_CONFIG", None):
            path = Path(path).resolve().expanduser()
            if path.is_file():
                return str(path)

            for extension in extensions:
                path = path / (name + extension)
                if path.is_file():
                    return str(path)
    else:
        dirs_to_check = [
            Path(path) if not isinstance(path, Path) else path for path in dirs_to_check
        ]
        if path := os.environ.get("PIPEDAG_CONFIG", None):
            logger = structlog.getLogger(module=__name__)
            logger.info(
                "ignoring PIPEDAG_CONFIG environment variable", PIPEDAG_CONFIG=path
            )

    # Search in other directories
    for path in dirs_to_check:
        path = Path(path)
        for extension in extensions:
            config_path = (path / (name + extension)).resolve()
            if config_path.is_file():
                return str(config_path)

    raise FileNotFoundError(
        f"No config file found with name={name}, extensions={extensions}"
    )


def load_config(path: str | Path) -> PipedagConfig:
    path = Path(path)
    path = path.expanduser()

    with open(path, encoding="utf-8") as fh:
        # TODO: we should read file in a different way such that we get line number information for every element
        #  in the dict
        pipedag_config_dict = yaml.safe_load(fh)

    # TODO: do some schema validation (i.e. with JSON Schema)
    assert (
        "name" in pipedag_config_dict
    ), f"Config file needs top level attribute 'name:': {path}"
    assert (
        "instances" in pipedag_config_dict or "flows" in pipedag_config_dict
    ), f"Config file needs top level attribute 'instances:' or 'flows:' {path}"
    return PipedagConfig(pipedag_config_dict, path)


def get_section(config_dict: dict[str, Any], section_path: list[str]):
    section = config_dict
    found = True
    for element in section_path:
        if element not in section:
            found = False
            break
        else:
            section = section[element]
    if not found:
        return None
    return section


def create_section(config_dict: dict[str, Any], section_path: list[str]):
    section = copy.deepcopy(config_dict)
    for element in reversed(section_path):
        section = {element: section}
    return section


def update_resolved_attributes(
    config_dict: dict[str, Any],
    pipedag_config_dict: dict[str, Any],
    resolved_attributes: tuple[dict, dict],
):
    resolved_attributes1 = copy.deepcopy(resolved_attributes[0])
    resolve_sections = [
        ([], "technical_setups"),
        (["table_store"], "table_store_connections"),
        (["blob_store"], "blob_store_connections"),
    ]
    for section_path, resolve_section_name in resolve_sections:
        # remove 's' at end of resolve section
        resolve_attribute = resolve_section_name[0:-1]
        section = get_section(config_dict, section_path)
        if section is None:
            continue
        if resolve_attribute in section:
            reference = section[resolve_attribute]
            if resolve_section_name not in pipedag_config_dict:
                raise AttributeError(
                    f"Could not find '{resolve_section_name}' section in pipedag config"
                    f" file; Found '{resolve_attribute}={reference}'"
                )
            if reference not in pipedag_config_dict[resolve_section_name]:
                raise AttributeError(
                    f"Could not find '{reference}' in section '{resolve_section_name}'"
                    f" within pipedag config file; Found '{resolve_attribute}:"
                    f" {reference}'"
                )
            resolved_attributes1[resolve_section_name] = create_section(
                pipedag_config_dict[resolve_section_name][reference], section_path
            )

            # Prevent recursive lookups
            if "technical_setup" in resolved_attributes1[resolve_section_name]:
                setup = resolved_attributes1[resolve_section_name]["technical_setup"]
                raise AttributeError(
                    "Attribute 'technical_setup' is not allowed in section"
                    f" '{resolve_section_name}' within pipedag config file; Found"
                    f" '{reference}: technical_setup: {setup}'"
                )
        # All attributes in resolve sections may be overridden by config_dict attributes
        for key in resolved_attributes1.get(resolve_section_name, {}):
            if key in section:
                resolved_attributes1[resolve_section_name][key] = deep_merge(
                    resolved_attributes1[resolve_section_name][key], section[key]
                )

    # lookup also resolved attributes in technical_setup section
    resolved_attributes2 = {}
    if "technical_setups" in resolved_attributes1:
        resolved_attributes2 = update_resolved_attributes(
            resolved_attributes1["technical_setups"], pipedag_config_dict, ({}, {})
        )[0]

    return resolved_attributes1, resolved_attributes2


def apply_resolved_attributes(
    config_dict: dict[str, Any], resolved_attributes: tuple[dict, dict]
):
    final_config_dict = copy.deepcopy(config_dict)
    assert "technical_setups" not in resolved_attributes[1]
    # We prioritize a directly referenced xxx_connection => resolved_attributes[0].
    # But we apply the technical_setup first, so connections both directly referenced or
    # 2nd prio, referenced by technical_setup can override attributes. We guarantee that never attributes of two
    # xxx_connection sections are mixed together. But we don't guarantee this if connection attributes are directly
    # mentioned in technical_setup or instance. Likewise, we guarantee that never attributes of two technical_setups
    # are mixed.
    for resolve_section_name in [
        "technical_setups",
        "table_store_connections",
        "blob_store_connections",
    ]:
        if resolve_section_name in resolved_attributes[0]:
            final_config_dict = deep_merge(
                final_config_dict, resolved_attributes[0][resolve_section_name]
            )
        elif resolve_section_name in resolved_attributes[1]:
            final_config_dict = deep_merge(
                final_config_dict, resolved_attributes[1][resolve_section_name]
            )
    return final_config_dict


def replace_environment_variables(attribute: str):
    ret = ""
    remainder = attribute
    try:
        while True:
            start_idx = remainder.index("{$")
            end_idx = remainder.index("}", start_idx)
            env_var = remainder[start_idx + 2 : end_idx]
            if env_var not in os.environ:
                raise AttributeError(
                    f"Could not find environment variable '{env_var}' referenced in:"
                    f" {attribute}"
                )
            ret += remainder[0:start_idx]
            ret += os.environ[env_var]
            remainder = remainder[end_idx + 1 :]
    except ValueError:
        ret += remainder  # no more "{$" found in string

    return ret


def get_username():
    for env_var in ["user", "USER", "username", "USERNAME"]:
        if env_var in os.environ:
            return os.environ[env_var]

    raise AttributeError(
        f"Failed detecting username from environment variables $USER or $USERNAME"
    )


def apply_per_user_id_change(per_user_template: str, _id: str):
    replacements = dict(id=_id, username=get_username())
    for placeholder in replacements:
        if placeholder not in per_user_template:
            raise AttributeError(
                f"palceholder '{placeholder}' is missing in `per_user_template`:"
                f" {per_user_template}"
            )
    return per_user_template.format(**replacements)


class PipedagConfig:
    _default_config = None

    def __init__(self, pipedag_config_dict: dict[str, Any], config_file: str | Path):
        self.pipedag_config_dict = pipedag_config_dict
        # TODO: provide good error messages about configuration with line number
        self.config_file = Path(config_file)
        self.logger = structlog.getLogger(module=__name__, cls=self.__class__.__name__)

    @classmethod
    def load(
        cls, path: Path | str | None = None, base_name: str | None = None
    ) -> PipedagConfig:
        store_default = False
        if path is None:
            if cls._default_config is not None and base_name is None:
                return cls._default_config
            else:
                path = find_config(name=base_name)
                if base_name is None:
                    store_default = True
        else:
            path = Path(path)
            if path.is_dir():
                path = find_config(name=base_name, dirs_to_check=[path])
        _config = load_config(path)
        if store_default:
            cls._default_config = _config
        return _config

    def get_pipedag_name(self):
        return self.pipedag_config_dict.get("name")

    def get(
        self, instance="__any__", per_user: bool = False, flow_name: str | None = None
    ):
        if flow_name is None:
            flow_name = self.get_pipedag_name()
        try:
            cfg = self.parse_pipedag_config(
                self.pipedag_config_dict, instance, per_user, flow_name
            )
        except (AttributeError, TypeError) as e:
            self.logger.exception(
                "Error parsing pipedag config",
                name=self.get_pipedag_name(),
                pipedag_config_file=self.config_file,
                pipedag_config=self.pipedag_config_dict,
            )
            raise AttributeError(str(e) + f": pipedag config file: {self.config_file}")
        return cfg

    @staticmethod
    def parse_pipedag_config(
        pipedag_config_dict: dict[str, Any],
        instance: str,
        per_user: bool,
        flow_name: str,
    ):
        config_dict = {}
        strict_instance = pipedag_config_dict.get("strict_instance_lookup", True)
        # state for attributes resolved by lookups in technical_setups, table_store_connections, ...
        resolved_attributes = ({}, {})
        override_sequence = [
            ["instances", "__any__"],
            ["instances", instance],
            ["flows", "__any__", "instances", "__any__"],
            ["flows", "__any__", "instances", instance],
            ["flows", flow_name, "instances", "__any__"],
            ["flows", flow_name, "instances", instance],
            ["flows", flow_name],
        ]
        found_instance = instance == "__any__"
        found_any_instance = False
        for instance_path in override_sequence:
            if strict_instance and "instances" in instance_path:
                idx = instance_path.index("instances") + 1
                instance_section = get_section(
                    pipedag_config_dict, instance_path[0:idx]
                )
                if instance_section is not None:
                    found_any_instance = found_any_instance or len(instance_section) > 0
                    found_instance = found_instance or instance in instance_section

            section = get_section(pipedag_config_dict, instance_path)
            if section is not None:
                config_dict = deep_merge(config_dict, section)
                # prepare state for apply_resolved_attributes and ensure that assignments in config_dict override
                # attributes that would be pulled from references
                resolved_attributes = update_resolved_attributes(
                    config_dict, pipedag_config_dict, resolved_attributes
                )

        # strict instance reference checking
        if strict_instance and not found_instance and found_any_instance:
            raise AttributeError(
                f"Could not find requested instance '{instance}' in pipedag config file"
            )

        # apply referenced attributes from technical_setups / xxx_connections;
        # we ensure that attributes of different references of the same type are never mixed
        config_dict = apply_resolved_attributes(config_dict, resolved_attributes)
        # Apply defaults
        pipedag_name = pipedag_config_dict.get("name", None)
        interface = config_dict.get("network_interface", "127.0.0.1")
        fail_fast = config_dict.get("fail_fast", False)
        attrs = config_dict.get("attrs", {})
        instance_id = config_dict.get("instance_id", flow_name)
        per_user_template = config_dict.get("per_user_template", "{id}_{username}")
        if per_user:
            instance_id = apply_per_user_id_change(per_user_template, instance_id)

        cfg = ConfigContext(
            pipedag_name=pipedag_name,
            flow_name=flow_name,
            instance_name=instance,
            instance_id=instance_id,
            config_dict=config_dict.copy(),
            fail_fast=fail_fast,
            network_interface=interface,
            attrs=attrs,
        )

        # make sure @cached_property store is already setup and loaded (this will throw config parsing errors earlier)
        _ = cfg.store
        # also try creating orchestration engine and locking_manager
        cfg.create_orchestration_engine().dispose()
        cfg.create_lock_manager().dispose()

        return cfg
