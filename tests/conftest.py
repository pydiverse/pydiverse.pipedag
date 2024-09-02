from __future__ import annotations

import itertools
import logging
import os
from collections import defaultdict
from pathlib import Path

import pytest

from pydiverse.pipedag.util.structlog import setup_logging

# Load the `run_with_instances` fixture, so it gets applied to all tests
from tests.fixtures.instances import INSTANCE_MARKS, fixture_run_with_instance
from tests.util.dask_patch import *  # noqa: F401,F403

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

log_level = logging.INFO if not os.environ.get("DEBUG", "") else logging.DEBUG
setup_logging(log_level=log_level)


# Pytest Configuration


@pytest.fixture(autouse=True, scope="function")
def structlog_test_info(request):
    """Add testcase information to structlog context"""
    if not os.environ.get("DEBUG", ""):
        yield
        return

    import structlog

    with structlog.contextvars.bound_contextvars(testcase=request.node.name):
        yield


def pytest_generate_tests(metafunc: pytest.Metafunc):
    # Parametrize tests based on `instances` and `skip_instances` mark.
    # Depends on the `fixture_run_with_instance` fixture to be imported at a
    #     global level.

    instances = {}
    kwargs = {}
    if mark := metafunc.definition.get_closest_marker("instances"):
        instances = dict.fromkeys(mark.args)
        kwargs = dict(mark.kwargs)
    if mark := metafunc.definition.get_closest_marker("skip_instances"):
        for instance in mark.args:
            if instance in instances:
                del instances[instance]

    if len(instances):
        params = []
        for instance in instances:
            marks = INSTANCE_MARKS.get(instance, ())
            params.append(
                pytest.param(
                    (instance, kwargs),
                    marks=marks,
                    id=instance,
                )
            )

        metafunc.parametrize("run_with_instance", params, indirect=True)


supported_options = [
    "postgres",  # see default_options
    "snowflake",
    "mssql",
    "ibm_db2",
    "duckdb",
    "pdtransform",
    "ibis",
    "polars",
    "dask",
    "prefect",
]

default_options = ["postgres"]


def pytest_addoption(parser):
    for opt in supported_options:
        parser.addoption(
            "--" + opt,
            action="store_true",
            default=False,
            help=f"run test that require {opt}",
        )
        if opt in default_options:
            # it seems the pytest parser is not automatically offering --no-flag
            parser.addoption(
                "--no-" + opt,
                action="store_true",
                default=False,
                help=f"run test that require {opt}",
            )


def pytest_collection_modifyitems(config: pytest.Config, items):
    for opt in supported_options:
        if not (
            config.getoption("--" + opt)
            or (opt in default_options and not config.getoption("--no-" + opt))
        ):
            skip = pytest.mark.skip(reason=f"{opt} not selected")
            for item in items:
                if opt in item.keywords:
                    item.add_marker(skip)


@pytest.hookimpl
def pytest_parallelize_group_items(config, items):
    groups = defaultdict(list)
    auto_group_iter = itertools.cycle([f"auto_{i}" for i in range(os.cpu_count() or 4)])
    for item in items:
        group = "DEFAULT"

        if hasattr(item, "get_closest_marker"):
            if marker := item.get_closest_marker("parallelize"):
                if marker.args:
                    group = marker.args[0]
                else:
                    group = next(auto_group_iter)

        if hasattr(item, "callspec"):
            if instance := item.callspec.params.get("run_with_instance"):
                # instance is a tuple of (instance_name, config_kwargs)
                group = instance[0]

        groups[group].append(item)
    return groups
