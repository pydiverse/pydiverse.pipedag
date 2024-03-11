from __future__ import annotations

import sqlalchemy as sa

from pydiverse.pipedag.backend.table.util import DType


def test_dtype_from_sqlalchemy():
    def assert_conversion(type_, expected):
        assert DType.from_sql(type_) == expected

    assert_conversion(sa.BigInteger(), DType.INT64)
    assert_conversion(sa.Integer(), DType.INT32)
    assert_conversion(sa.SmallInteger(), DType.INT16)

    assert_conversion(sa.Numeric(), DType.FLOAT64)
    assert_conversion(sa.Numeric(13, 2), DType.FLOAT64)
    assert_conversion(sa.Numeric(1, 0), DType.FLOAT64)
    assert_conversion(sa.DECIMAL(13, 2), DType.FLOAT64)
    assert_conversion(sa.DECIMAL(1, 0), DType.FLOAT64)
    assert_conversion(sa.Float(), DType.FLOAT64)
    assert_conversion(sa.Float(24), DType.FLOAT32)
    assert_conversion(sa.Float(53), DType.FLOAT64)

    assert_conversion(sa.String(), DType.STRING)
    assert_conversion(sa.Boolean(), DType.BOOLEAN)

    assert_conversion(sa.Date(), DType.DATE)
    assert_conversion(sa.Time(), DType.TIME)
    assert_conversion(sa.DateTime(), DType.DATETIME)


def test_dtype_to_sqlalchemy():
    def assert_conversion(type_: DType, expected):
        assert isinstance(type_.to_sql(), expected)

    assert_conversion(DType.INT64, sa.BigInteger)
    assert_conversion(DType.INT32, sa.Integer)
    assert_conversion(DType.INT16, sa.SmallInteger)
    assert_conversion(DType.INT8, sa.SmallInteger)

    assert_conversion(DType.UINT64, sa.BigInteger)
    assert_conversion(DType.UINT32, sa.BigInteger)
    assert_conversion(DType.UINT16, sa.Integer)
    assert_conversion(DType.UINT8, sa.SmallInteger)

    assert_conversion(DType.FLOAT64, sa.Float)
    assert_conversion(DType.FLOAT32, sa.Float)

    assert_conversion(DType.STRING, sa.String)
    assert_conversion(DType.BOOLEAN, sa.Boolean)

    assert_conversion(DType.DATE, sa.Date)
    assert_conversion(DType.TIME, sa.Time)
    assert_conversion(DType.DATETIME, sa.DateTime)
