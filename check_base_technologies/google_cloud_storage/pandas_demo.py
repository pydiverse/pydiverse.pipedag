# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

"""
GCS read/write using Pandas with fsspec storage_options.
Uses gcsfs under the hood when storage_options are omitted and path is gs://.
"""

from __future__ import annotations


def run(base_url: str) -> None:
    import pandas as pd

    base = base_url.rstrip("/") + "/"
    path = f"{base}pandas_demo.parquet"

    from _shared import get_sample_dataframe_pandas

    df = get_sample_dataframe_pandas()

    # Write: pandas uses fsspec for gs:// when gcsfs is installed
    df.to_parquet(path)

    # Read
    back = pd.read_parquet(path)
    pd.testing.assert_frame_equal(back, df)


if __name__ == "__main__":
    import sys

    run(sys.argv[1] if len(sys.argv) > 1 else "gs://test-bucket-pipedag-actions-public/gcs_access_demo/")
