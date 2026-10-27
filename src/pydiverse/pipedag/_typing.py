# Copyright (c) QuantCo and pydiverse contributors 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, Union

if TYPE_CHECKING:
    from pydiverse.pipedag import Blob, Table
    from pydiverse.pipedag.materialize.store import BaseTableStore
    from pydiverse.pipedag.materialize.table_hook_base import TableHookResolver

T = TypeVar("T")
StoreT = TypeVar("StoreT", bound="BaseTableStore")
TableHookResolverT = TypeVar("TableHookResolverT", bound="TableHookResolver")

# Signature (`P`) and return type (`R`) of functions decorated with @materialize.
# R1/R2/R3 are the element types of `nout=2` / `nout=3` tuple returns.
P = ParamSpec("P")
R = TypeVar("R")
R1 = TypeVar("R1")
R2 = TypeVar("R2")
R3 = TypeVar("R3")

# Accepted by `input_type` / `as_type`: a type, or a type with a backend such as
# `(pd.DataFrame, "arrow")` / `{"type": pd.DataFrame, "backend": "arrow"}`
InputType = type | tuple[type, str] | dict[str, Any]

if TYPE_CHECKING:

    class AnyBase(Any):
        """Base class that makes type checkers accept instances wherever any type is expected."""

else:
    # At run time, `Any` as a base class would add the metaclass `typing._AnyMeta`, which
    # conflicts with other metaclasses (e.g. pydiverse.colspec's `ColSpecMeta`).
    class AnyBase:
        pass


# Materializable
MPrimitives = int | float | bool | str
MTypes = Union["Table", "Blob"]

BaseMaterializable = MPrimitives | MTypes
Materializable = (
    BaseMaterializable | dict[str, "Materializable"] | list["Materializable"] | tuple["Materializable", ...]
)
