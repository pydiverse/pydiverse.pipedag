from __future__ import annotations

import pytest


@pytest.hookspec()
def pytest_parallelize_group_items(config, items):
    ...
