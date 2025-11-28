# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

try:
    import tidypolars
    from tidypolars import Tibble
except ImportError:
    tidypolars = None
    Tibble = None
