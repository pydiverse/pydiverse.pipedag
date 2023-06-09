from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pydiverse.pipedag.util.structlog import setup_logging

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
    os.environ["MSSQL_PASSWORD"] = "PydiQuant27"

    os.environ["PYDIVERSE_PIPEDAG_PYTEST"] = "1"


setup_environ()
setup_logging()


# @pytest.fixture(
#     autouse=True,
#     scope="function",
# )
# def structlog_test_info(request):
#     """Add testcase information to structlog context"""
#     with structlog.contextvars.bound_contextvars(testcase=request.node.name):
#         yield


# Pytest Configuration


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
            if instance := callspec.params.get("instance"):
                group = instance

        groups[group].append(item)
    return groups
