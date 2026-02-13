#!/usr/bin/env python3
# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

"""
Run all GCS access backends and report which work and which fail.

Usage:
  python run_all.py [BASE_URL] [--verbose] [--fail-fast]

  BASE_URL    e.g. gs://your-bucket/prefix/  (default from GCS_DEMO_BUCKET env, or gs://test-bucket-pipedag-actions-public/gcs_access_demo/)
  --verbose   after summary, print full tracebacks for every failure
  --fail-fast stop on first failure and print full traceback (no summary table)
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback
from collections.abc import Callable

# Ensure this directory is on path so imports work when run from repo root
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

BACKENDS: list[tuple[str, Callable[[str], None]]] = []


def _load_backends():
    import duckdb_fsspec_demo
    import duckdb_httpfs_demo
    import fsspec_demo
    import pandas_demo
    import polars_demo
    import pyarrow_demo

    global BACKENDS
    BACKENDS = [
        ("FSSpec", fsspec_demo.run),
        ("Pandas", pandas_demo.run),
        ("Polars", polars_demo.run),
        ("PyArrow", pyarrow_demo.run),
        ("DuckDB (FSSpec)", duckdb_fsspec_demo.run),
        ("DuckDB (HTTPFS)", duckdb_httpfs_demo.run),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run GCS read/write demos for all backends and report results.")
    parser.add_argument(
        "base_url",
        nargs="?",
        default=None,
        help="Base GCS URL (e.g. gs://bucket/prefix/). Default: gs://${GCS_DEMO_BUCKET}/gcs_access_demo/",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print full traceback for every failure after the summary.",
    )
    parser.add_argument(
        "--fail-fast",
        "-f",
        action="store_true",
        help="Stop on first failure and print full traceback (no summary).",
    )
    args = parser.parse_args()

    bucket = os.environ.get("GCS_DEMO_BUCKET", "test-bucket-pipedag-actions-public")
    base_url = args.base_url or f"gs://{bucket}/gcs_access_demo/"
    if not base_url.startswith("gs://"):
        base_url = f"gs://{base_url}"
    base_url = base_url.rstrip("/") + "/"

    _load_backends()

    results: list[tuple[str, bool, str | None, str | None]] = []  # name, ok, short_err, full_tb

    for name, run_fn in BACKENDS:
        if args.fail_fast:
            run_fn(base_url)
            print(f"  {name}: OK")
            continue

        try:
            run_fn(base_url)
            results.append((name, True, None, None))
        except Exception as e:
            short = f"{type(e).__name__}: {e}"
            full = traceback.format_exc()
            results.append((name, False, short, full))

    if args.fail_fast:
        return 0

    # Summary table
    print("GCS access backend results")
    print("Base URL:", base_url)
    print()
    max_name = max(len(n) for n, *_ in results) if results else 10
    fmt = f"  {{:{max_name}}}  {{}}"
    for name, ok, short_err, _ in results:
        status = "OK" if ok else "FAIL"
        detail = "" if ok else f" — {short_err}"
        print(fmt.format(name, status + detail))
    print()

    n_ok = sum(1 for _, ok, *_ in results if ok)
    n_fail = len(results) - n_ok
    print(f"Passed: {n_ok}  Failed: {n_fail}")

    if n_fail and args.verbose:
        print("\n" + "=" * 60 + "\nFull tracebacks for failures:\n")
        for name, ok, _short, full_tb in results:
            if not ok and full_tb:
                print(f"--- {name} ---")
                print(full_tb)
                print()

    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
