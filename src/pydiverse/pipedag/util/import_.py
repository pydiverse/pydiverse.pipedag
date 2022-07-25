from __future__ import annotations

import functools
from typing import Any, Type


def requires(requirements: Any | list, exception: BaseException | type[BaseException]):
    """Class decorator for handling optional imports.

    If any of the requirements are falsy, this decorator prevents the class
    from being instantiated and raises the provided exception instead.
    """

    if not isinstance(requirements, (list, tuple)):
        requirements = (requirements,)

    def decorator(cls):
        if not all(requirements):

            @functools.wraps(cls.__new__)
            def raiser(*args, **kwargs):
                raise exception

            cls.__new__ = raiser
        return cls

    return decorator
