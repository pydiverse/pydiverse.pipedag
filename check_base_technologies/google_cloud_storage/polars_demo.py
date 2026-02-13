# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

"""
GCS read/write using Polars native backend.
Note: Polars native GCS does not support impersonated ADC (only service_account / authorized_user).
When impersonated ADC is detected, falls back to PyArrow (same strategy as pydiverse.pipedag).
"""

from __future__ import annotations


def _is_polars_gcs_credential_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return (
        "impersonated_service_account" in msg
        or ("object-store error" in msg and "gcs" in msg)
        or "generic gcs error" in msg
    )


def run(base_url: str) -> None:
    import polars as pl

    base = base_url.rstrip("/") + "/"
    path = f"{base}polars_demo.parquet"

    from _shared import get_sample_dataframe_polars

    df = get_sample_dataframe_polars()

    # Write — try Polars native, fall back to PyArrow if impersonated ADC
    try:
        df.write_parquet(path)
    except OSError as e:
        if not _is_polars_gcs_credential_error(e):
            raise
        import fsspec
        import pyarrow as pa
        import pyarrow.fs
        import pyarrow.parquet as pq

        fs = fsspec.filesystem("gs")
        pyarrow_fs = pa.fs.PyFileSystem(pa.fs.FSSpecHandler(fs))
        pq.write_table(df.to_arrow(), path.split("://", 1)[1], filesystem=pyarrow_fs)

    # Read — try Polars native, fall back to PyArrow if impersonated ADC
    try:
        back = pl.read_parquet(path)
    except OSError as e:
        if not _is_polars_gcs_credential_error(e):
            raise
        import fsspec
        import pyarrow as pa
        import pyarrow.fs
        import pyarrow.parquet as pq

        fs = fsspec.filesystem("gs")
        pyarrow_fs = pa.fs.PyFileSystem(pa.fs.FSSpecHandler(fs))
        tbl = pq.read_table(path.split("://", 1)[1], filesystem=pyarrow_fs)
        back = pl.from_arrow(tbl)

    assert back.equals(df), "Read back data did not match"


if __name__ == "__main__":
    import sys

    run(sys.argv[1] if len(sys.argv) > 1 else "gs://test-bucket-pipedag-actions-public/gcs_access_demo/")
