# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

"""
GCS read/write using DuckDB's built-in HTTPFS extension.
Requires HMAC keys for private buckets (set GCS_KEY_ID and GCS_SECRET env vars),
or works with public buckets without secrets.
"""

from __future__ import annotations

import os


def run(base_url: str) -> None:
    import duckdb

    base = base_url.rstrip("/") + "/"
    path = f"{base}duckdb_httpfs_demo.parquet"

    conn = duckdb.connect(":memory:")

    # Load httpfs and optionally create GCS secret from HMAC env vars
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")

    key_id = os.environ.get("GCS_KEY_ID")
    secret = os.environ.get("GCS_SECRET")
    if key_id and secret:
        # Escape single quotes in secret for SQL
        secret_esc = secret.replace("\\", "\\\\").replace("'", "''")
        key_id_esc = key_id.replace("\\", "\\\\").replace("'", "''")
        conn.execute(f"CREATE SECRET (TYPE GCS, KEY_ID '{key_id_esc}', SECRET '{secret_esc}')")

    # Write: create table and export to GCS
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
