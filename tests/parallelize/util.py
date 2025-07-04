# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause


def parse_config(config, name):
    return getattr(config.option, name, config.getini(name))
