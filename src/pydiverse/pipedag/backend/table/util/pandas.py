from __future__ import annotations

import pandas as pd


def adj_pandas_types(df: pd.DataFrame):
    df = df.copy()
    for col in df.dtypes.loc[lambda x: x == int].index:
        df[col] = df[col].astype(pd.Int64Dtype())
    for col in df.dtypes.loc[lambda x: x == bool].index:
        df[col] = df[col].astype(pd.BooleanDtype())
    for col in df.dtypes.loc[lambda x: x == object].index:
        df[col] = df[col].astype(pd.StringDtype())
    return df
