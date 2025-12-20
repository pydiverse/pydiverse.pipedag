# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

from packaging.version import Version

try:
    import prefect

    prefect_version = Version(prefect.__version__)
except ImportError:
    prefect = None
    prefect_version = Version("0")
