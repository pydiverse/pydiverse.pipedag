# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

from typing import TYPE_CHECKING, ParamSpec, TypeVar, Union

if TYPE_CHECKING:
    from pydiverse.pipedag import Blob, Table
    from pydiverse.pipedag.materialize.store import BaseTableStore
    from pydiverse.pipedag.materialize.table_hook_base import TableHookResolver

T = TypeVar("T")
StoreT = TypeVar("StoreT", bound="BaseTableStore")
TableHookResolverT = TypeVar("TableHookResolverT", bound="TableHookResolver")

# Used by the @materialize decorator to preserve the signature of the decorated
# function. `P` captures the parameter list, `R` the (declaration-time) return type.
# R1/R2/R3 spell out the element types of `nout=2` / `nout=3` tuple returns, which is
# the only way to give tuple-unpacking assignments a checkable arity.
P = ParamSpec("P")
R = TypeVar("R")
R1 = TypeVar("R1")
R2 = TypeVar("R2")
R3 = TypeVar("R3")

# Materializable
MPrimitives = int | float | bool | str
MTypes = Union["Table", "Blob"]

BaseMaterializable = MPrimitives | MTypes
Materializable = (
    BaseMaterializable | dict[str, "Materializable"] | list["Materializable"] | tuple["Materializable", ...]
)
