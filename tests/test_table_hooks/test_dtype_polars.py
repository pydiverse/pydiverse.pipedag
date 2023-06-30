from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pydiverse.pipedag.backend.table.util import DType

pl = pytest.importorskip("polars")

if TYPE_CHECKING:
    import polars as pl


def test_dtype_from_polars():
    def assert_conversion(type_, expected):
        assert DType.from_polars(type_) == expected

    assert_conversion(pl.Int64, DType.INT64)
    assert_conversion(pl.Int32, DType.INT32)
    assert_conversion(pl.Int16, DType.INT16)
    assert_conversion(pl.Int8, DType.INT8)

    assert_conversion(pl.UInt64, DType.UINT64)
    assert_conversion(pl.UInt32, DType.UINT32)
    assert_conversion(pl.UInt16, DType.UINT16)
    assert_conversion(pl.UInt8, DType.UINT8)

    assert_conversion(pl.Float64, DType.FLOAT64)
    assert_conversion(pl.Float32, DType.FLOAT32)

    assert_conversion(pl.Utf8, DType.STRING)
    assert_conversion(pl.Boolean, DType.BOOLEAN)

    assert_conversion(pl.Date, DType.DATE)
    assert_conversion(pl.Time, DType.TIME)
    assert_conversion(pl.Datetime, DType.DATETIME)
    assert_conversion(pl.Datetime("ms"), DType.DATETIME)
    assert_conversion(pl.Datetime("us"), DType.DATETIME)
    assert_conversion(pl.Datetime("ns"), DType.DATETIME)


def test_dtype_to_polars():
    def assert_conversion(type_: DType, expected):
        assert type_.to_polars() == expected

    assert_conversion(DType.INT64, pl.Int64)
    assert_conversion(DType.INT32, pl.Int32)
    assert_conversion(DType.INT16, pl.Int16)
    assert_conversion(DType.INT8, pl.Int8)

    assert_conversion(DType.UINT64, pl.UInt64)
    assert_conversion(DType.UINT32, pl.UInt32)
    assert_conversion(DType.UINT16, pl.UInt16)
    assert_conversion(DType.UINT8, pl.UInt8)

    assert_conversion(DType.FLOAT64, pl.Float64)
    assert_conversion(DType.FLOAT32, pl.Float32)

    assert_conversion(DType.STRING, pl.Utf8)
    assert_conversion(DType.BOOLEAN, pl.Boolean)

    assert_conversion(DType.DATE, pl.Date)
    assert_conversion(DType.TIME, pl.Time)
    assert_conversion(DType.DATETIME, pl.Datetime("us"))
