from __future__ import annotations

from typing import TYPE_CHECKING, Callable, TypeVar, Union

if TYPE_CHECKING:
    from pydiverse.pipedag import Blob, Table
    from pydiverse.pipedag.backend import BaseTableStore


def decorator_hint(decorator: Callable) -> Callable:
    # Used to fix incorrect type hints in pycharm
    return decorator


T = TypeVar("T")
CallableT = TypeVar("CallableT", bound=Callable)
StoreT = TypeVar("StoreT", bound="BaseTableStore")

# Materializable
MPrimitives = Union[int, float, bool, str]
MTypes = Union["Table", "Blob"]

BaseMaterializable = Union[MPrimitives, MTypes]
Materializable = Union[
    BaseMaterializable,
    dict[str, "Materializable"],
    list["Materializable"],
    tuple["Materializable", ...],
]
