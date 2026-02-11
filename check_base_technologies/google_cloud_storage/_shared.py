# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

"""Shared test data and helpers for GCS demo backends."""

import io


# Small table used for write-then-read in all backends (parquet bytes for raw fsspec)
def get_sample_parquet_bytes():
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [10.0, 20.0, 30.0]})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def get_sample_table_pyarrow():
    import pyarrow as pa

    return pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [10.0, 20.0, 30.0]})


def get_sample_dataframe_pandas():
    import pandas as pd

    return pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [10.0, 20.0, 30.0]})


def get_sample_dataframe_polars():
    import polars as pl

    return pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [10.0, 20.0, 30.0]})
