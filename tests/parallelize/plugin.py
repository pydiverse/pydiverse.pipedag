from __future__ import annotations

import pytest

from .sesson import Session
from .util import parse_config


def pytest_addoption(parser):
    workers_help = (
        "Set the max num of workers (aka processes) to start "
        "(int or 'auto' - one per core)"
    )

    group = parser.getgroup("parallelize")
    group.addoption("--workers", dest="workers", help=workers_help)
    parser.addini("workers", workers_help)


@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    workers = parse_config(config, "workers")
    if config.option.collectonly or not workers:
        return

    config.pluginmanager.register(Session(config), "parallelize-session")

    try:
        # Patch _jb_pytest_runner to support parallel execution of test
        # when using the PyCharm IDE
        from _jb_runner_tools import set_parallel_mode

        set_parallel_mode()
    except ImportError:
        pass


@pytest.hookimpl
def pytest_addhooks(pluginmanager):
    from . import hooks

    pluginmanager.add_hookspecs(hooks)
