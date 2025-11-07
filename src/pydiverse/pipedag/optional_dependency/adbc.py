# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

try:
    import adbc_driver_postgresql as adbc_postgres
except ImportError:
    adbc_postgres = None
