from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import structlog

if TYPE_CHECKING:
    from _pytest.python import Session, Parser


# Setup


def setup_environ():
    os.environ["PIPEDAG_CONFIG"] = str(Path(__file__).parent / "resources")
    os.environ["POSTGRES_USERNAME"] = "sa"
    os.environ["POSTGRES_PASSWORD"] = "Pydiverse23"
    os.environ["MSSQL_USERNAME"] = "sa"
    os.environ["MSSQL_PASSWORD"] = "PidyQuant27"

    os.environ["PYDIVERSE_PIPEDAG_PYTEST"] = "1"


def setup_structlog():
    _log_level = logging.INFO
    _log_stream = sys.stdout  # use sys.stderr with `pytest -s in ci.yml`
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
            structlog.processors.TimeStamper(),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(_log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(_log_stream),
        cache_logger_on_first_use=True,
    )


setup_environ()
setup_structlog()


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
