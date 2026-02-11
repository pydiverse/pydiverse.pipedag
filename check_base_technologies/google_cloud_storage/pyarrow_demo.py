# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

"""
GCS read/write using PyArrow with fsspec-wrapped filesystem.
Works with ADC, service account, and impersonated credentials.
"""

from __future__ import annotations


def run(base_url: str) -> None:
    import pyarrow.fs
    import pyarrow.parquet as pq

    base = base_url.rstrip("/") + "/"
    path = f"{base}pyarrow_demo.parquet"

    import fsspec

    fsspec_fs = fsspec.filesystem("gs")
    pyarrow_fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fsspec_fs))

    from _shared import get_sample_table_pyarrow

    table = get_sample_table_pyarrow()

    # Write
    pq.write_table(table, path, filesystem=pyarrow_fs)

    # Read
    back = pq.read_table(path, filesystem=pyarrow_fs)
    assert back.equals(table), "Read back data did not match"


if __name__ == "__main__":
    import sys

    run(sys.argv[1] if len(sys.argv) > 1 else "gs://test-bucket-pipedag-actions-public/gcs_access_demo/")
