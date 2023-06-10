from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path

import pytest

from pydiverse.pipedag.util.structlog import setup_logging

# Load the `run_with_instances` fixture, so it gets applied to all tests
from tests.fixtures.instances import INSTANCE_MARKS, fixture_run_with_instance

_ = fixture_run_with_instance


pytest_plugins = ["tests.parallelize.plugin"]


# Setup


def setup_environ():
    if "PIPEDAG_CONFIG" not in os.environ:
        os.environ["PIPEDAG_CONFIG"] = str(Path(__file__).parent / "resources")
    os.environ["POSTGRES_USERNAME"] = "sa"
    os.environ["POSTGRES_PASSWORD"] = "Pydiverse23"
    os.environ["MSSQL_USERNAME"] = "sa"
    os.environ["MSSQL_PASSWORD"] = "PydiQuant27"

    os.environ["PYDIVERSE_PIPEDAG_PYTEST"] = "1"


setup_environ()
setup_logging()


# Pytest Configuration


def pytest_generate_tests(metafunc: pytest.Metafunc):
    # Parametrize tests based on `instances` and `skip_instances` mark.
    # Depends on the `fixture_run_with_instance` fixture to be imported at a
    #     global level.

    instances = {}
    if mark := metafunc.definition.get_closest_marker("instances"):
        instances = dict.fromkeys(mark.args)
    if mark := metafunc.definition.get_closest_marker("skip_instances"):
        for instance in mark.args:
            if instance in instances:
                del instances[instance]

    if len(instances):
        params = []
        for instance in instances:
            if instance in INSTANCE_MARKS:
                params.append(pytest.param(instance, marks=INSTANCE_MARKS[instance]))
            else:
                params.append(instance)

        metafunc.parametrize("run_with_instance", params, indirect=True)


def pytest_addoption(parser):
    for opt in ["mssql", "ibm_db2", "pdtransform", "ibis", "polars"]:
        parser.addoption(
            "--" + opt,
            action="store_true",
            default=False,
            help=f"run test that require {opt}",
        )


def pytest_collection_modifyitems(config: pytest.Config, items):
    for opt in ["mssql", "ibm_db2", "pdtransform", "ibis", "polars"]:
        if not config.getoption("--" + opt):
            skip = pytest.mark.skip(reason=f"{opt} not selected")
            for item in items:
                if opt in item.keywords:
                    item.add_marker(skip)


@pytest.hookimpl
def pytest_parallelize_group_items(config, items):
    groups = defaultdict(list)
    for item in items:
        group = "DEFAULT"

        if callspec := getattr(item, "callspec", None):
            if instance := callspec.params.get("run_with_instance"):
                group = instance

        groups[group].append(item)
    return groups
