# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

"""
GCS read/write using DuckDB with a registered FSSpec (gcsfs) filesystem.
Use this path when using ADC, OIDC, or impersonated credentials.
"""

from __future__ import annotations


def run(base_url: str) -> None:
    import duckdb
    import fsspec

    base = base_url.rstrip("/") + "/"
    path = f"{base}duckdb_fsspec_demo.parquet"

    conn = duckdb.connect(":memory:")
    fs = fsspec.filesystem("gs")
    conn.register_filesystem(fs)

    # Write: create a table and export to GCS as parquet
    conn.execute(
        "CREATE TABLE t AS SELECT 1 AS id, 'a' AS name, 10.0 AS value "
        "UNION ALL SELECT 2, 'b', 20.0 UNION ALL SELECT 3, 'c', 30.0"
    )
    conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")

    # Read back
    out = conn.execute(f"SELECT * FROM read_parquet('{path}')").fetchall()
    assert len(out) == 3
    assert out[0] == (1, "a", 10.0)


if __name__ == "__main__":
    import sys

    run(sys.argv[1] if len(sys.argv) > 1 else "gs://test-bucket-pipedag-actions-public/gcs_access_demo/")
