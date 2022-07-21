from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    Tuple,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    from pdpipedag.core.container import Table, Blob


def decorator_hint(decorator: Callable) -> Callable:
    # Used to fix incorrect type hints in pycharm
    return decorator


T = TypeVar("T")
CallableT = TypeVar("CallableT", bound=Callable)

# Materialisable

MPrimitives = Union[int, float, bool, str]
MTypes = Union["Table", "Blob"]

BaseMaterialisable = Union[MPrimitives, MTypes]
Materialisable = Union[
    BaseMaterialisable,
    Dict[MPrimitives, "Materialisable"],
    List["Materialisable"],
    Tuple["Materialisable", ...],
]
