# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

from upath import UPath


def is_file_uri(path: UPath):
    return path.as_uri().startswith("file://")
