from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, List, Tuple, TypeVar, Union

if TYPE_CHECKING:
    pass


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
    Dict[str, "Materializable"],
    List["Materializable"],
    Tuple["Materializable", ...],
]
