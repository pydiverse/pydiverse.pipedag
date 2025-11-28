# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import types
from typing import Generic, TypeVar

try:
    import dataframely as dy
    from dataframely._base_schema import SchemaMeta
    from dataframely._polars import FrameType
    from dataframely.random import Generator
    from dataframely.testing import validation_mask
except ImportError:

    class Generator:
        pass

    T = TypeVar("T")

    class DyDataFrame(Generic[T]):
        pass

    class DyDummyClass:
        def __init__(self, *args, **kwargs):
            pass

    FrameType = None
    SchemaMeta = None
    validation_mask = None
    dy = types.ModuleType("dataframely")
    dy.DataFrame = DyDataFrame
    dy.LazyFrame = DyDataFrame
    dy.FailureInfo = None
    dy.Column = None
    dy.Collection = object
    dy.Schema = DyDummyClass
    dy.filter = lambda: lambda fn: fn  # noqa
    dy.rule = lambda: lambda fn: fn  # noqa
    for _type in [
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float64",
        "Bool",
        "String",
        "Decimal",
        "Enum",
        "Struct",
        "List",
        "Date",
        "Datetime",
        "Time",
        "Duration",
        "Float",
        "Integer",
    ]:
        setattr(dy, _type, DyDummyClass)
