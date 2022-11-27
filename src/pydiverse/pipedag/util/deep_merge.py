"""Generic deep update function for nested dictionaries.

Seems to be solved already in various ways (do we like an extra dependency for pydantic.deep_update?)
https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
But for snippets, license restrictions exist:
https://www.ictrecht.nl/en/blog/what-is-the-license-status-of-stackoverflow-code-snippets
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping


def deep_merge(x, y):
    cls = type(x)
    cls2 = type(y)
    assert (
        cls == cls2
    ), f"deep_merge failed due to mismatching types {cls} vs. {cls2}: '{x}' vs. '{y}'"

    if isinstance(x, Mapping):
        z = _deep_merge_dict(x, y)
    elif isinstance(x, Iterable) and not isinstance(x, str):
        z = _deep_merge_iterable(x, y)
    else:
        z = y  # update

    return z


def _deep_merge_iterable(x: Iterable, y: Iterable):
    return [deep_merge(a, b) for a, b in zip(x, y)]


def _deep_merge_dict(x: Mapping, y: Mapping):
    z = dict(x)
    for key, value in x.items():
        if key in y:
            z[key] = deep_merge(x[key], y[key])
    z.update({key: value for key, value in y.items() if key not in z})
    return z
