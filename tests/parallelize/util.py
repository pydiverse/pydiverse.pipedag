from __future__ import annotations


def parse_config(config, name):
    return getattr(config.option, name, config.getini(name))
