from __future__ import annotations

import pyarrow as pa

from pydiverse.pipedag.backend.table.util import DType


def test_dtype_from_pyarrow():
    def assert_conversion(type_, expected):
        assert DType.from_arrow(type_) == expected

    assert_conversion(pa.int64(), DType.INT64)
    assert_conversion(pa.int32(), DType.INT32)
    assert_conversion(pa.int16(), DType.INT16)
    assert_conversion(pa.int8(), DType.INT8)

    assert_conversion(pa.uint64(), DType.UINT64)
    assert_conversion(pa.uint32(), DType.UINT32)
    assert_conversion(pa.uint16(), DType.UINT16)
    assert_conversion(pa.uint8(), DType.UINT8)

    assert_conversion(pa.float64(), DType.FLOAT64)
    assert_conversion(pa.float32(), DType.FLOAT32)
    assert_conversion(pa.float16(), DType.FLOAT32)

    assert_conversion(pa.string(), DType.STRING)
    assert_conversion(pa.bool_(), DType.BOOLEAN)

    assert_conversion(pa.date32(), DType.DATE)
    assert_conversion(pa.date64(), DType.DATE)

    assert_conversion(pa.time32("s"), DType.TIME)
    assert_conversion(pa.time32("ms"), DType.TIME)
    assert_conversion(pa.time64("us"), DType.TIME)
    assert_conversion(pa.time64("ns"), DType.TIME)

    assert_conversion(pa.timestamp("s"), DType.DATETIME)
    assert_conversion(pa.timestamp("ms"), DType.DATETIME)
    assert_conversion(pa.timestamp("us"), DType.DATETIME)
    assert_conversion(pa.timestamp("ns"), DType.DATETIME)


def test_dtype_to_pyarrow():
    def assert_conversion(type_: DType, expected):
        assert type_.to_arrow() == expected

    assert_conversion(DType.INT64, pa.int64())
    assert_conversion(DType.INT32, pa.int32())
    assert_conversion(DType.INT16, pa.int16())
    assert_conversion(DType.INT8, pa.int8())

    assert_conversion(DType.UINT64, pa.uint64())
    assert_conversion(DType.UINT32, pa.uint32())
    assert_conversion(DType.UINT16, pa.uint16())
    assert_conversion(DType.UINT8, pa.uint8())

    assert_conversion(DType.FLOAT64, pa.float64())
    assert_conversion(DType.FLOAT32, pa.float32())

    assert_conversion(DType.STRING, pa.string())
    assert_conversion(DType.BOOLEAN, pa.bool_())

    assert_conversion(DType.DATE, pa.date32())
    assert_conversion(DType.TIME, pa.time64("us"))
    assert_conversion(DType.DATETIME, pa.timestamp("us"))
