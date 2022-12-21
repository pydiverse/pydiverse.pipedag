"""Generic deep update function for nested dictionaries.

Seems to be solved already in various ways (do we like an extra dependency for pydantic.deep_update?)
https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
But for snippets, license restrictions exist:
https://www.ictrecht.nl/en/blog/what-is-the-license-status-of-stackoverflow-code-snippets
"""  # noqa: E501
from __future__ import annotations

from collections.abc import Iterable, Mapping


def deep_merge(x, y, check_enum=False):
    if type(x) != type(y):
        raise TypeError(
            f"deep_merge failed doe to type mismatch '{x}' (type: {type(x)}) vs. '{y}'"
            f" (type: {type(y)})"
        )

    if isinstance(x, Mapping):
        z = _deep_merge_dict(x, y)
    elif isinstance(x, Iterable) and not isinstance(x, str):
        z = _deep_merge_iterable(x, y)
    else:
        z = y  # update

    return z


def _deep_merge_iterable(x: Iterable, y: Iterable):
    # Merging lists is not trivial.
    # There are a few different strategies: replace, unique, append, intersection, ...
    return y
    # return [*x, *y]
    # return [deep_merge(a, b) for a, b in zip(x, y)]


def _deep_merge_dict(x: Mapping, y: Mapping):
    z = dict(x)
    for key in x:
        if key in y:
            z[key] = deep_merge(x[key], y[key])
    z.update({key: value for key, value in y.items() if key not in z})
    return z
