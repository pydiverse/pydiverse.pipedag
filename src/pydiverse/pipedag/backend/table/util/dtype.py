from __future__ import annotations

from enum import Enum, auto

import numpy as np
import pandas as pd
import pyarrow as pa
import sqlalchemy as sa


class PandasDTypeBackend(str, Enum):
    NUMPY = "numpy"
    ARROW = "arrow"


class DType(Enum):
    """

    Type Translation:

        DType <-> SQLAlchemy
        DType <-> Pandas
        DType <-> Arrow
        DType <-> Polars

    """

    # Integer Types
    INT8 = auto()
    INT16 = auto()
    INT32 = auto()
    INT64 = auto()

    UINT8 = auto()
    UINT16 = auto()
    UINT32 = auto()
    UINT64 = auto()

    # Float Types
    FLOAT32 = auto()
    FLOAT64 = auto()

    # Date/Time
    DATE = auto()
    TIME = auto()
    DATETIME = auto()

    # Other
    STRING = auto()
    BOOLEAN = auto()

    @staticmethod
    def from_sql(type_) -> DType:
        if isinstance(type_, sa.SmallInteger):
            return DType.INT16
        if isinstance(type_, sa.BigInteger):
            return DType.INT64
        if isinstance(type_, sa.Integer):
            return DType.INT32
        if isinstance(type_, sa.Numeric):
            precision = type_.precision or 53
            if precision <= 24:
                return DType.FLOAT32
            return DType.FLOAT64
        if isinstance(type_, sa.String):
            return DType.STRING
        if isinstance(type_, sa.Boolean):
            return DType.BOOLEAN
        if isinstance(type_, sa.Date):
            return DType.DATE
        if isinstance(type_, sa.Time):
            return DType.TIME
        if isinstance(type_, sa.DateTime):
            return DType.DATETIME

        raise TypeError

    @staticmethod
    def from_pandas(type_) -> DType:
        if isinstance(type_, pd.ArrowDtype):
            return DType.from_arrow(type_.pyarrow_dtype)

        def is_np_dtype(type_, np_dtype):
            return pd.core.dtypes.common._is_dtype_type(
                type_, pd.core.dtypes.common.classes(np_dtype)
            )

        if pd.api.types.is_signed_integer_dtype(type_):
            if is_np_dtype(type_, np.int64):
                return DType.INT64
            elif is_np_dtype(type_, np.int32):
                return DType.INT32
            elif is_np_dtype(type_, np.int16):
                return DType.INT16
            elif is_np_dtype(type_, np.int8):
                return DType.INT8
            raise TypeError
        if pd.api.types.is_unsigned_integer_dtype(type_):
            if is_np_dtype(type_, np.uint64):
                return DType.UINT64
            elif is_np_dtype(type_, np.uint32):
                return DType.UINT32
            elif is_np_dtype(type_, np.uint16):
                return DType.UINT16
            elif is_np_dtype(type_, np.uint8):
                return DType.UINT8
            raise TypeError
        if pd.api.types.is_float_dtype(type_):
            if is_np_dtype(type_, np.float64):
                return DType.FLOAT64
            elif is_np_dtype(type_, np.float32):
                return DType.FLOAT32
            raise TypeError
        if pd.api.types.is_string_dtype(type_):
            # We reserve the use of the object column for string.
            return DType.STRING
        if pd.api.types.is_bool_dtype(type_):
            return DType.BOOLEAN
        if pd.api.types.is_datetime64_any_dtype(type_):
            return DType.DATETIME

        raise TypeError

    @staticmethod
    def from_arrow(type_) -> DType:
        if pa.types.is_signed_integer(type_):
            if pa.types.is_int64(type_):
                return DType.INT64
            if pa.types.is_int32(type_):
                return DType.INT32
            if pa.types.is_int16(type_):
                return DType.INT16
            if pa.types.is_int8(type_):
                return DType.INT8
            raise TypeError
        if pa.types.is_unsigned_integer(type_):
            if pa.types.is_uint64(type_):
                return DType.UINT64
            if pa.types.is_uint32(type_):
                return DType.UINT32
            if pa.types.is_uint16(type_):
                return DType.UINT16
            if pa.types.is_uint8(type_):
                return DType.UINT8
            raise TypeError
        if pa.types.is_floating(type_):
            if pa.types.is_float64(type_):
                return DType.FLOAT64
            if pa.types.is_float32(type_):
                return DType.FLOAT32
            if pa.types.is_float16(type_):
                return DType.FLOAT32
            raise TypeError
        if pa.types.is_string(type_):
            return DType.STRING
        if pa.types.is_boolean(type_):
            return DType.BOOLEAN
        if pa.types.is_timestamp(type_):
            return DType.DATETIME
        if pa.types.is_date(type_):
            return DType.DATE
        if pa.types.is_time(type_):
            return DType.TIME
        raise TypeError

    @staticmethod
    def from_polars(type_) -> DType:
        import polars as pl

        return {
            pl.Int64: DType.INT64,
            pl.Int32: DType.INT32,
            pl.Int16: DType.INT16,
            pl.Int8: DType.INT8,
            pl.UInt64: DType.UINT64,
            pl.UInt32: DType.UINT32,
            pl.UInt16: DType.UINT16,
            pl.UInt8: DType.UINT8,
            pl.Float64: DType.FLOAT64,
            pl.Float32: DType.FLOAT32,
            pl.Utf8: DType.STRING,
            pl.Boolean: DType.BOOLEAN,
            pl.Datetime: DType.DATETIME,
            pl.Time: DType.TIME,
            pl.Date: DType.DATE,
        }[type_.base_type()]

    def to_sql(self):
        return {
            DType.INT8: sa.SmallInteger(),
            DType.INT16: sa.SmallInteger(),
            DType.INT32: sa.Integer(),
            DType.INT64: sa.BigInteger(),
            DType.UINT8: sa.SmallInteger(),
            DType.UINT16: sa.Integer(),
            DType.UINT32: sa.BigInteger(),
            DType.UINT64: sa.BigInteger(),
            DType.FLOAT32: sa.Float(24),
            DType.FLOAT64: sa.Float(53),
            DType.STRING: sa.String(),
            DType.BOOLEAN: sa.Boolean(),
            DType.DATE: sa.Date(),
            DType.TIME: sa.Time(),
            DType.DATETIME: sa.DateTime(),
        }[self]

    def to_pandas(self, backend: PandasDTypeBackend = PandasDTypeBackend.ARROW):
        if backend == PandasDTypeBackend.NUMPY:
            return self.to_pandas_nullable()
        if backend == PandasDTypeBackend.ARROW:
            return pd.ArrowDtype(self.to_arrow())

    def to_pandas_nullable(self):
        if self == DType.TIME:
            raise TypeError("pandas doesn't have a native time dtype")

        return {
            DType.INT8: pd.Int8Dtype(),
            DType.INT16: pd.Int16Dtype(),
            DType.INT32: pd.Int32Dtype(),
            DType.INT64: pd.Int64Dtype(),
            DType.UINT8: pd.UInt8Dtype(),
            DType.UINT16: pd.UInt16Dtype(),
            DType.UINT32: pd.UInt32Dtype(),
            DType.UINT64: pd.UInt64Dtype(),
            DType.FLOAT32: pd.Float32Dtype(),
            DType.FLOAT64: pd.Float64Dtype(),
            DType.STRING: pd.StringDtype(),
            DType.BOOLEAN: pd.BooleanDtype(),
            DType.DATE: "datetime64[ns]",
            # DType.TIME not supported
            DType.DATETIME: "datetime64[ns]",
        }[self]

    def to_arrow(self):
        return {
            DType.INT8: pa.int8(),
            DType.INT16: pa.int16(),
            DType.INT32: pa.int32(),
            DType.INT64: pa.int64(),
            DType.UINT8: pa.uint8(),
            DType.UINT16: pa.uint16(),
            DType.UINT32: pa.uint32(),
            DType.UINT64: pa.uint64(),
            DType.FLOAT32: pa.float32(),
            DType.FLOAT64: pa.float64(),
            DType.STRING: pa.string(),
            DType.BOOLEAN: pa.bool_(),
            DType.DATE: pa.date32(),
            DType.TIME: pa.time64("us"),
            DType.DATETIME: pa.timestamp("us"),
        }[self]

    def to_polars(self):
        import polars as pl

        return {
            DType.INT64: pl.Int64,
            DType.INT32: pl.Int32,
            DType.INT16: pl.Int16,
            DType.INT8: pl.Int8,
            DType.UINT64: pl.UInt64,
            DType.UINT32: pl.UInt32,
            DType.UINT16: pl.UInt16,
            DType.UINT8: pl.UInt8,
            DType.FLOAT64: pl.Float64,
            DType.FLOAT32: pl.Float32,
            DType.STRING: pl.Utf8,
            DType.BOOLEAN: pl.Boolean,
            DType.DATETIME: pl.Datetime("us"),
            DType.TIME: pl.Time,
            DType.DATE: pl.Date,
        }[self]
