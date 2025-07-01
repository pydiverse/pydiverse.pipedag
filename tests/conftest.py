# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import itertools
import logging
import os
from collections import defaultdict
from pathlib import Path

import pytest

from pydiverse.common.util.structlog import setup_logging
from pydiverse.pipedag.util.testing_s3 import initialize_test_s3_bucket

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
    os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"

    from fsspec.config import conf  # fsspec’s global config

    conf.setdefault("s3", {})
    conf["s3"].update(
        {
            "key": "minioadmin",
            "secret": "minioadmin",
            "client_kwargs": {
                "endpoint_url": "http://localhost:9000",
            },
            # ─── botocore.config.Config parameters go here ────
            "config_kwargs": {
                "connect_timeout": 3,
                "read_timeout": 5,
                "retries": {"max_attempts": 2, "mode": "standard"},
                "s3": {"addressing_style": "path"},  # MinIO needs path-style
            },
        }
    )


setup_environ()

log_level = (
    logging.ERROR
    if os.environ.get("ERROR_ONLY", "0") != "0"
    else logging.INFO
    if os.environ.get("DEBUG", "0") == "0"
    else logging.DEBUG
)
setup_logging(log_level=log_level)


# Pytest Configuration


@pytest.fixture(autouse=True, scope="function")
def structlog_test_info(request):
    """Add testcase information to structlog context"""
    if os.environ.get("DEBUG", "0") == "0" and os.environ.get("LOG_TEST_NAME", "0") == "0":
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
    "s3",
]

default_options = ["postgres", "duckdb", "polars"]

sub_backends = dict(duckdb=["parquet_backend"], s3=["parquet_s3_backend"])


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
        if not (config.getoption("--" + opt) or (opt in default_options and not config.getoption("--no-" + opt))):
            all_opts = [opt]
            if opt in sub_backends:
                all_opts.extend(sub_backends[opt])
            for opt2 in all_opts:
                skip = pytest.mark.skip(reason=f"{opt2} not selected")
                for item in items:
                    if (
                        opt2 in item.keywords
                        or any(itm.startswith(opt2 + "-") for itm in item.keywords)
                        or any(itm.endswith("-" + opt2) for itm in item.keywords)
                        or any("-" + opt2 + "-" in itm for itm in item.keywords)
                    ):
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


def pytest_sessionstart(session):
    # initialize the S3 test bucket in case --s3 option was given
    if session.config.getoption("--s3"):
        initialize_test_s3_bucket()
