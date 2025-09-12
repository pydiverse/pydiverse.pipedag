# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import types

try:
    import pydiverse.colspec as cs
except ImportError:
    cs = types.ModuleType("pydiverse.colspec")
    fn = lambda *args, **kwargs: (lambda *args, **kwargs: None)  # noqa: E731
    cs.__getattr__ = lambda name: object if name in ["ColSpec", "Collection"] else fn
