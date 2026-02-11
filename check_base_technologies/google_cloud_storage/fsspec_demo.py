# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

"""
GCS read/write using FSSpec (gcsfs).
Works with ADC, service account, and impersonated credentials.
"""

from __future__ import annotations


def run(base_url: str) -> None:
    import fsspec

    base = base_url.rstrip("/") + "/"
    path = f"{base}fsspec_demo.parquet"

    fs = fsspec.filesystem("gs")
    from _shared import get_sample_parquet_bytes

    data = get_sample_parquet_bytes()

    # Write
    with fs.open(path, "wb") as f:
        f.write(data)

    # Read
    with fs.open(path, "rb") as f:
        read_back = f.read()

    assert read_back == data, "Read back data did not match written data"


if __name__ == "__main__":
    import sys

    run(sys.argv[1] if len(sys.argv) > 1 else "gs://test-bucket-pipedag-actions-public/gcs_access_demo/")
