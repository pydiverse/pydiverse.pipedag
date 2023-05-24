from __future__ import annotations


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
    """State object for creating non-colliding names."""

    def __init__(self, prefix):
        self.prefix = prefix
        self.names = set()
        self.cnt = 1

    def get_name(self, name_wish: str | None):
        if name_wish is not None and name_wish not in self.names:
            self.names.add(name_wish)
            return name_wish
        else:
            for _ in range(100):
                name = f"{self.prefix}{self.cnt}"
                if name not in self.names:
                    self.names.add(name)
                    return name
                self.cnt += 1
            raise RuntimeError(f"Failed to find unique name: wish={name_wish}")
