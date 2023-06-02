from __future__ import annotations

import builtins
import importlib
from typing import Any


def requires(requirements: Any | list, exception: BaseException | type[BaseException]):
    """Class decorator for handling optional imports.

    If any of the requirements are falsy, this decorator prevents the class
    from being instantiated and any class attributes from being accessed,
    and raises the provided exception instead.
    """

    if not isinstance(requirements, (list, tuple)):
        requirements = (requirements,)

    def decorator(cls):
        if all(requirements):
            return cls

        # Modify class to raise exception
        class RaiserMeta(type):
            def __getattr__(self, x):
                raise exception

        def raiser(*args, **kwargs):
            raise exception

        __name = str(cls.__name__)
        __bases = ()
        __dict = {
            "__metaclass__": RaiserMeta,
            "__wrapped__": cls,
            "__new__": raiser,
        }

        return RaiserMeta(__name, __bases, __dict)

    return decorator


def import_object(import_path: str):
    """Loads a class given an import path

    >>> # An import statement like this
    >>> from pandas import DataFrame
    >>> # can be expressed as follows:
    >>> import_object("pandas.DataFrame")
    """

    parts = [part for part in import_path.split(".") if part]
    module, n = None, 0

    while n < len(parts):
        try:
            module = importlib.import_module(".".join(parts[: n + 1]))
            n = n + 1
        except ImportError:
            break

    obj = module or builtins
    for part in parts[n:]:
        obj = getattr(obj, part)

    return obj


def load_object(config_dict: dict):
    """Instantiates an instance of an object given

    The import path (module.Class) should be specified as the "class" value
    of the dict. The args section of the dict get used as the instance config.

    If the class defines a `_init_conf_` function, it gets called using the
    config values, otherwise they just get passed to the class initializer.

    >>> # module.Class(argument="value")
    >>> load_object({
    >>>     "class": "module.Class",
    >>>     "args": {
    >>>         "argument": "value",
    >>>     },
    >>> })
    """

    if "class" not in config_dict:
        raise RuntimeError(
            "Attribute 'class' is missing in configuration "
            "section that supports multiple backends\n"
            f"config section: {config_dict}"
        )
    cls = import_object(config_dict["class"])

    args = config_dict.get("args", {})
    if not isinstance(args, dict):
        raise TypeError(
            f"Invalid type for args section: {type(args)}\n"
            f"config section: {config_dict}"
        )
    try:
        init_conf = cls._init_conf_
        return init_conf(args)
    except AttributeError:
        return cls(**args)
