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
    from pdpipedag.core.table import Table


def decorator_hint(decorator: Callable) -> Callable:
    # Used to fix incorrect type hints in pycharm
    return decorator


T = TypeVar('T')

# Materialisable

MPrimitives = Union[
    int,
    float,
    bool,
    str
]
MTypes = Union['Table']

BaseMaterialisable = Union[MPrimitives, MTypes]
Materialisable = Union[
    BaseMaterialisable,
    Dict[MPrimitives, 'Materialisable'],
    List['Materialisable'],
    Tuple['Materialisable', ...]
]

