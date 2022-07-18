import os

import pytest
import pandas as pd
import sqlalchemy as sa
from prefect import Flow

import pdpipedag
from pdpipedag import materialise, Schema, Table
from pdpipedag.backend import PipeDAGStore
from pdpipedag.backend.table import DictTableStore, SQLTableStore


# Configure

engine = sa.create_engine(f'postgresql://127.0.0.1/pipedag', echo = False)

pdpipedag.config = pdpipedag.configuration.Config(
    store = PipeDAGStore(
        table = SQLTableStore(engine),
        blob = None,
        lock = None,
    )
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

    @materialise(input_type = pd.DataFrame)
    def double_values(df: pd.DataFrame):
        return Table(df.transform(lambda x: x * 2))

    @materialise(input_type = pd.DataFrame)
    def join_on_a(left: pd.DataFrame, right: pd.DataFrame):
        return Table(left.merge(right, how = 'left', on = 'a'))

    @materialise(input_type = pd.DataFrame)
    def list_arg(x: list[pd.DataFrame]):
        assert isinstance(x[0], pd.DataFrame)
        return Table(x[0])

    with Flow('FLOW') as flow:
        with Schema('SCHEMA1'):
            b, a = inputs()
            a2 = double_values(a)
            b2 = double_values(b)
            b4 = double_values(b2)
            b4 = double_values(b4)
            x = list_arg([a2, b, b4])

        with Schema('SCHEMA2'):
            xj = join_on_a(x, b4)
            a = double_values(xj)

    result = flow.run()
    assert result.is_successful()
