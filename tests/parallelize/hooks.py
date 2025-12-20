# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import pytest


@pytest.hookspec()
def pytest_parallelize_group_items(config, items): ...
