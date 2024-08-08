from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from pydiverse.pipedag.backend.table.util import DType, PandasDTypeBackend


def test_dtype_from_pandas():
    def assert_conversion(type_, expected):
        assert DType.from_pandas(type_) == expected

    assert_conversion(int, DType.INT64)
    assert_conversion(float, DType.FLOAT64)
    assert_conversion(str, DType.STRING)
    assert_conversion(bool, DType.BOOLEAN)

    # Objects should get converted to string
    assert_conversion(object, DType.STRING)
    assert_conversion(dt.date, DType.STRING)
    assert_conversion(dt.time, DType.STRING)
    assert_conversion(dt.datetime, DType.STRING)

    # Numpy types
    assert_conversion(np.int64, DType.INT64)
    assert_conversion(np.int32, DType.INT32)
    assert_conversion(np.int16, DType.INT16)
    assert_conversion(np.int8, DType.INT8)

    assert_conversion(np.uint64, DType.UINT64)
    assert_conversion(np.uint32, DType.UINT32)
    assert_conversion(np.uint16, DType.UINT16)
    assert_conversion(np.uint8, DType.UINT8)

    assert_conversion(np.floating, DType.FLOAT64)
    assert_conversion(np.float64, DType.FLOAT64)
    assert_conversion(np.float32, DType.FLOAT32)

    assert_conversion(np.bytes_, DType.STRING)
    assert_conversion(np.bool_, DType.BOOLEAN)

    assert_conversion(np.datetime64, DType.DATETIME)
    assert_conversion(np.dtype("datetime64[ms]"), DType.DATETIME)
    assert_conversion(np.dtype("datetime64[ns]"), DType.DATETIME)

    # Numpy nullable extension types
    assert_conversion(pd.Int64Dtype(), DType.INT64)
    assert_conversion(pd.Int32Dtype(), DType.INT32)
    assert_conversion(pd.Int16Dtype(), DType.INT16)
    assert_conversion(pd.Int8Dtype(), DType.INT8)

    assert_conversion(pd.UInt64Dtype(), DType.UINT64)
    assert_conversion(pd.UInt32Dtype(), DType.UINT32)
    assert_conversion(pd.UInt16Dtype(), DType.UINT16)
    assert_conversion(pd.UInt8Dtype(), DType.UINT8)

    assert_conversion(pd.Float64Dtype(), DType.FLOAT64)
    assert_conversion(pd.Float32Dtype(), DType.FLOAT32)

    assert_conversion(pd.StringDtype(), DType.STRING)
    assert_conversion(pd.BooleanDtype(), DType.BOOLEAN)


def test_dtype_to_pandas_numpy():
    def assert_conversion(type_: DType, expected):
        assert type_.to_pandas(PandasDTypeBackend.NUMPY) == expected

    assert_conversion(DType.INT64, pd.Int64Dtype())
    assert_conversion(DType.INT32, pd.Int32Dtype())
    assert_conversion(DType.INT16, pd.Int16Dtype())
    assert_conversion(DType.INT8, pd.Int8Dtype())

    assert_conversion(DType.UINT64, pd.UInt64Dtype())
    assert_conversion(DType.UINT32, pd.UInt32Dtype())
    assert_conversion(DType.UINT16, pd.UInt16Dtype())
    assert_conversion(DType.UINT8, pd.UInt8Dtype())

    assert_conversion(DType.STRING, pd.StringDtype())
    assert_conversion(DType.BOOLEAN, pd.BooleanDtype())

    assert_conversion(DType.DATE, np.dtype("datetime64[ns]"))
    assert_conversion(DType.DATETIME, np.dtype("datetime64[ns]"))

    with pytest.raises(TypeError):
        DType.TIME.to_pandas(PandasDTypeBackend.NUMPY)


@pytest.mark.skipif("pd.__version__ < '2'")
def test_dtype_to_pandas_pyarrow():
    def assert_conversion(type_: DType, expected):
        if isinstance(expected, pa.DataType):
            assert type_.to_pandas(PandasDTypeBackend.ARROW) == pd.ArrowDtype(expected)
        else:
            assert type_.to_pandas(PandasDTypeBackend.ARROW) == expected

    assert_conversion(DType.INT64, pa.int64())
    assert_conversion(DType.INT32, pa.int32())
    assert_conversion(DType.INT16, pa.int16())
    assert_conversion(DType.INT8, pa.int8())

    assert_conversion(DType.UINT64, pa.uint64())
    assert_conversion(DType.UINT32, pa.uint32())
    assert_conversion(DType.UINT16, pa.uint16())
    assert_conversion(DType.UINT8, pa.uint8())

    assert_conversion(DType.STRING, pd.StringDtype(storage="pyarrow"))
    assert_conversion(DType.BOOLEAN, pa.bool_())

    assert_conversion(DType.DATE, pa.date32())
    assert_conversion(DType.TIME, pa.time64("us"))
    assert_conversion(DType.DATETIME, pa.timestamp("us"))
