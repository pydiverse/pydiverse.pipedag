# Copyright (c) QuantCo and pydiverse contributors 2026-2026
# SPDX-License-Identifier: BSD-3-Clause

"""Checks that the `old` pixi environment resolves every dependency exactly at its lower bound.

The `old` environment uses the `lowest` solve strategy to test the lower bounds declared in
pixi.toml (and mirrored in pyproject.toml). If the solver selects a higher version than the
declared floor, the floor is not tested and should be raised to the resolved version.
"""

import sys
import tomllib
from pathlib import Path

import yaml
from packaging.version import InvalidVersion, Version

ENVIRONMENT = "old"
# CI only runs the tests of the old environment on linux. Other platforms may lack builds of the
# lowest versions, which would force raising the floors for all platforms.
PLATFORM = "linux-64"
PACKAGE_NAME = "pydiverse-pipedag"


def normalize(name: str) -> str:
    # conda-forge uses both `-` and `_` in package names (e.g. arrow-odbc vs. arrow_odbc)
    return name.lower().replace("_", "-")


def lower_bound(spec: str) -> str | None:
    """Returns the `>=` floor of a version spec, or None if the spec has no lower bound."""
    floor = None
    for part in spec.split(","):
        part = part.strip()
        if part.startswith(">="):
            floor = part.removeprefix(">=").strip()
        elif not part.startswith(("<", "!=")) and part != "*":
            # e.g. `>1.0`, `~=1.0`, `==1.0` or `1.0.*` bound the version from below as well
            raise ValueError(f"unsupported version spec '{spec}', declare the lower bound with '>='")
    return floor


def matches_floor(floor: str, resolved: str) -> bool:
    try:
        # trailing zeros are ignored (e.g. `>=3.11` matches `3.11.0`, but not `3.11.1`)
        return Version(floor) == Version(resolved)
    except InvalidVersion:
        return floor == resolved


def environment_extras(pixi: dict) -> set[str]:
    environment = pixi["environments"][ENVIRONMENT]
    if isinstance(environment, list):
        environment = {"features": environment}
    features = [pixi["feature"][name] for name in environment["features"]]
    if not environment.get("no-default-feature", False):
        # the default feature is defined at the top level of pixi.toml
        features.append(pixi)

    extras = set()
    for feature in features:
        dependency = feature.get("dependencies", {}).get(PACKAGE_NAME)
        if isinstance(dependency, dict):
            extras.update(dependency.get("extras", []))
    return extras


def declared_floors(pixi: dict) -> dict[str, tuple[str, str]]:
    """Returns {package: (floor, origin)} for all package dependencies installed in the environment."""
    package = pixi["package"]
    tables = {"run-dependencies": package["run-dependencies"]}
    for extra in sorted(environment_extras(pixi)):
        tables[f"extra '{extra}'"] = package["extra-dependencies"][extra]

    floors = {}
    for origin, dependencies in tables.items():
        for name, spec in dependencies.items():
            if isinstance(spec, dict):
                spec = spec.get("version", "*")
            try:
                floor = lower_bound(spec)
            except ValueError as e:
                raise ValueError(f"{name} ({origin}): {e}") from None
            # dependencies without a lower bound (e.g. `setuptools = "<81"`) are skipped
            if floor is not None:
                floors.setdefault(normalize(name), (floor, origin))
    return floors


def resolved_versions(lock: dict) -> dict[str, str]:
    """Returns {package: version} for the conda packages in the environment."""
    versions = {}
    for package in lock["environments"][ENVIRONMENT]["packages"][PLATFORM]:
        url = package.get("conda", "")
        if not url.startswith("https://"):
            continue
        filename = url.rsplit("/", 1)[-1].removesuffix(".conda").removesuffix(".tar.bz2")
        name, version, _build = filename.rsplit("-", 2)
        versions[normalize(name)] = version
    return versions


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    pixi = tomllib.loads((root / "pixi.toml").read_text())
    lock = yaml.safe_load((root / "pixi.lock").read_text())

    try:
        floors = declared_floors(pixi)
    except ValueError as e:
        print(e)
        return 1

    versions = resolved_versions(lock)
    exit_code = 0
    for name, (floor, origin) in sorted(floors.items()):
        if name not in versions:
            print(f"{name} ({origin}): not part of the {ENVIRONMENT} environment on {PLATFORM}")
            exit_code = 1
        elif not matches_floor(floor, versions[name]):
            print(
                f"{name} ({origin}): declares '>={floor}', but the {ENVIRONMENT} environment resolves "
                f"{versions[name]} on {PLATFORM}. Raise the lower bound in pixi.toml and pyproject.toml."
            )
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
