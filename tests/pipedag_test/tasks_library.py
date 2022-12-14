from __future__ import annotations

import pandas as pd

from pydiverse.pipedag import Blob, Table, materialize


@materialize(input_type=pd.DataFrame)
def noop(x):
    return x


@materialize(nout=2)
def create_tuple(x, y):
    return x, y


@materialize
def one():
    return 1


@materialize
def two():
    return 2


@materialize
def assert_equal(x, y):
    assert x == y
    return x, y


@materialize(input_type=pd.DataFrame)
def assert_table_equal(x, y):
    pd.testing.assert_frame_equal(x, y)


@materialize
def assert_blob_equal(x, y):
    assert x == y


@materialize
def simple_dataframe():
    df = pd.DataFrame(
        {
            "col1": [0, 1, 2, 3],
            "col2": ["0", "1", "2", "3"],
        }
    )
    return Table(df)


@materialize
def pd_dataframe(data: dict[str, list]):
    df = pd.DataFrame(data)
    return Table(df)


@materialize
def as_blob(x):
    return Blob(x)


class _SomeClass:
    def __eq__(self, other):
        return self.__dict__ == other.__dict__


@materialize
def object_blob(x: dict):
    instance = _SomeClass()
    instance.__dict__.update(x)

    return Blob(instance)


@materialize
def exception(x, r: bool):
    if r:
        raise Exception("THIS EXCEPTION IS EXPECTED")
    return x
