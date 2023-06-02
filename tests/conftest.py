from __future__ import annotations

import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import structlog

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


def setup_structlog(
    _log_level=logging.INFO,
    _log_stream=sys.stderr,
    timestamp_format="%Y-%m-%d %H:%M:%S.%f",
):
    logging.basicConfig(
        stream=_log_stream,
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=_log_level,
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt=timestamp_format),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(_log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(_log_stream),
        cache_logger_on_first_use=True,
    )


setup_environ()
setup_structlog(_log_stream=sys.stdout)  # use sys.stderr with `pytest -s in ci.yml`


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
