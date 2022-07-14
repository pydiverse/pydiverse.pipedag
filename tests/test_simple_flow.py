import os

import pytest
import pandas as pd
import sqlalchemy as sa
from prefect import Flow

import pdpipedag
from pdpipedag import materialise, Schema, Table
from pdpipedag.backend.table import SQLTableStorage, DictTableStorage


# Configure

# sqlite_path = os.path.abspath(os.path.join(os.path.curdir, 'test.sqlite'))
# engine = sa.create_engine(f'sqlite:///{sqlite_path}')
# engine.connect()
# table_storage = SQLTableStorage(engine)

table_storage = DictTableStorage()

pdpipedag.config = pdpipedag.configuration.Config(
    table_backend = table_storage,
)


def test_simple_flow():

    @materialise(nout = 2)
    def inputs():
        dfA = pd.DataFrame({
            'a': [0, 1, 2, 4],
            'b': [9, 8, 7, 6],
        })

        dfB = pd.DataFrame({
            'a': [2, 1, 0, 1],
            'x': [1, 1, 2, 2],
        })

        return Table(dfA, 'dfA'), Table(dfB, 'dfB')

    @materialise(type = pd.DataFrame)
    def double_values(df: pd.DataFrame):
        return Table(df.transform(lambda x: x * 2))

    @materialise(type = pd.DataFrame)
    def join_on_a(left: pd.DataFrame, right: pd.DataFrame):
        return Table(left.merge(right, how = 'left', on = 'a'))

    @materialise(type = pd.DataFrame)
    def list_arg(x: list[pd.DataFrame]):
        assert isinstance(x[0], pd.DataFrame)
        return Table(x[0])


    with Flow('FLOW') as flow:
        with Schema('SCHEMA1'):
            a, b = inputs()
            a2 = double_values(a)
            b2 = double_values(b)

            b4 = double_values(b2)
            x = list_arg([a, b, b4])

        with Schema('SCHEMA2'):
            x = join_on_a(a2, b4)

    result = flow.run()
    print(result.result[x].result.obj)
    assert result.is_successful()
