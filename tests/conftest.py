from __future__ import annotations

import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pydiverse.pipedag.util.config import setup_structlog

if TYPE_CHECKING:
    pass


pytest_plugins = ["tests.parallelize.plugin"]


# Setup


def setup_environ():
    if "PIPEDAG_CONFIG" not in os.environ:
        os.environ["PIPEDAG_CONFIG"] = str(Path(__file__).parent / "resources")
    os.environ["POSTGRES_USERNAME"] = "sa"
    os.environ["POSTGRES_PASSWORD"] = "Pydiverse23"
    os.environ["MSSQL_USERNAME"] = "sa"
    os.environ["MSSQL_PASSWORD"] = "PidyQuant27"

    os.environ["PYDIVERSE_PIPEDAG_PYTEST"] = "1"


setup_environ()
setup_structlog(_log_stream=sys.stdout)  # use sys.stderr with `pytest -s in ci.yml`


# Pytest Configuration


def pytest_addoption(parser):
    parser.addoption(
        "--mssql",
        action="store_true",
        default=False,
        help="run test that require mssql",
    )
    parser.addoption(
        "--ibm_db2",
        action="store_true",
        default=False,
        help="run test that require ibm_db2",
    )


def pytest_collection_modifyitems(config: pytest.Config, items):
    if not config.getoption("--mssql"):
        skip_mssql = pytest.mark.skip(reason="mssql not selected")
        for item in items:
            if "mssql" in item.keywords:
                item.add_marker(skip_mssql)

    if not config.getoption("--ibm_db2"):
        skip_ibm_db2 = pytest.mark.skip(reason="ibm_db2 not selected")
        for item in items:
            if "ibm_db2" in item.keywords:
                item.add_marker(skip_ibm_db2)


@pytest.hookimpl
def pytest_parallelize_group_items(config, items):
    groups = defaultdict(list)
    for item in items:
        group = "DEFAULT"

        if callspec := getattr(item, "callspec", None):
            if instance := callspec.params.get("instance"):
                group = instance

        groups[group].append(item)
    return groups
