# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import types

try:
    # pydot should be an optional dependency
    import pydot
except ImportError:
    pydot = types.ModuleType("pydot")
    pydot.Dot = None
