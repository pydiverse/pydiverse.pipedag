# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import types

try:
    import ibis
except ImportError:
    ibis = types.ModuleType("ibis")
    ibis.api = types.ModuleType("ibis.api")
    ibis.api.Table = None
