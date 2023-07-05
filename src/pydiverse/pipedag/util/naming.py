from __future__ import annotations

import itertools


def normalize_name(name: str) -> str:
    """Normalizes an identifier

    All names in PipeDAG are case-insensitive and can't contain any
    slashes. This helper function does exactly this conversion.
    """
    if name is not None:
        return name.casefold().strip().replace("/", "_")


def safe_name(name: str) -> str:
    """Converts an identifier to one that is lowercase, ascii only

    Some backends might only support a limited set of characters for
    identifiers. This generic functions provides a mechanism for making
    a name safe (at least in most bases) by encoding non ascii characters
    using punycode.

    :param name: The identifier / name to make safe
    :return: The safe name
    """
    name = normalize_name(name)
    name = name.encode("punycode").decode("ascii")
    return name


class NameDisambiguator:
    """State object for creating non-colliding names

    This object is used inside `TableHook.retrieve` to prevent SQLAlchemy issues...
    """

    def __init__(self):
        self.used_names = set()
        self.counter = itertools.count()

    def get_name(self, name: str | None) -> str:
        new_name = name
        while new_name in self.used_names:
            new_name = f"alias_{next(self.counter)}"

        self.used_names.add(new_name)
        return new_name
