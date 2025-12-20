# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar, Union

if TYPE_CHECKING:
    from pydiverse.pipedag import Blob, Table
    from pydiverse.pipedag.materialize.store import BaseTableStore
    from pydiverse.pipedag.materialize.table_hook_base import TableHookResolver


def decorator_hint(decorator: Callable) -> Callable:
    # Used to fix incorrect type hints in pycharm
    return decorator


T = TypeVar("T")
CallableT = TypeVar("CallableT", bound=Callable)
StoreT = TypeVar("StoreT", bound="BaseTableStore")
TableHookResolverT = TypeVar("TableHookResolverT", bound="TableHookResolver")

# Materializable
MPrimitives = int | float | bool | str
MTypes = Union["Table", "Blob"]

BaseMaterializable = MPrimitives | MTypes
Materializable = (
    BaseMaterializable | dict[str, "Materializable"] | list["Materializable"] | tuple["Materializable", ...]
)
