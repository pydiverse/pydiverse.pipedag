# GCS Access Examples

Standalone examples for reading/writing Google Cloud Storage (GCS) using different Python backends.

## Backends

| Backend        | Read | Write | Notes |
|----------------|------|-------|--------|
| **FSSpec**     | ✓    | ✓     | Uses `gcsfs`; works with ADC, service account, impersonation |
| **Pandas**     | ✓    | ✓     | Via fsspec storage_options |
| **Polars**     | ✓    | ✓     | Native or fsspec; native does not support impersonated ADC |
| **PyArrow**    | ✓    | ✓     | Via fsspec-wrapped filesystem |
| **DuckDB FSSpec**  | ✓ | ✓     | `register_filesystem(fsspec("gs"))`; use this with ADC/OIDC |
| **DuckDB HTTPFS**  | ✓ | ✓     | Built-in; requires HMAC keys or public buckets |

## Setup

From the `check_base_technologies/google_cloud_storage` directory, use pixi (conda-only, no pip):

```bash
cd check_base_technologies/google_cloud_storage
pixi install
```

Authenticate with GCS (pick one):

- **Application Default Credentials (ADC):** `gcloud auth application-default login`
- **Service account key file:** set `GOOGLE_APPLICATION_CREDENTIALS` to the JSON path
- **HTTPFS (DuckDB only):** create HMAC keys in GCP Console and set `GCS_KEY_ID`, `GCS_SECRET` (or use env in the script)

## Usage

Set your bucket and optional prefix (default prefix: `gcs_access_demo/`):

```bash
export GCS_DEMO_BUCKET=your-bucket-name
pixi run run
```

Or pass the base URL on the command line:

```bash
pixi run run gs://your-bucket-name/optional/prefix/
```

### Modes

- **Summary (default):** Run all backends, print a table of which passed/failed, and show short error messages.
- **Verbose errors:** `pixi run run-verbose -- gs://my-bucket/` — after the summary, print full tracebacks for every failure.
- **Fail fast:** `pixi run run-fail-fast -- gs://my-bucket/` — stop on first failure and print the full traceback (no summary table).

### Examples

```bash
# Summary only
pixi run run gs://my-bucket/

# Summary + full tracebacks for failures
pixi run run-verbose -- gs://my-bucket/

# Stop at first failure with full traceback
pixi run run-fail-fast -- gs://my-bucket/
```

## Optional: Run a single backend

From the `check_base_technologies/google_cloud_storage` directory:

```bash
pixi run python fsspec_demo.py gs://my-bucket/
pixi run python duckdb_httpfs_demo.py gs://my-bucket/
```

## Relation to pydiverse.pipedag

The same GCS backends (FSSpec, Pandas, PyArrow, DuckDB via FSSpec, and Polars with optional PyArrow fallback for impersonated credentials) are used by pydiverse.pipedag’s parquet table store. The main repo’s default pixi environment includes the `s3` feature (gcsfs) so that the GCS materialize test (`test_materialize_table_gcs`) runs with the same working stack as this demo. DuckDB HTTPFS is not used by pipedag for GCS; it is shown here for comparison and requires HMAC keys for private buckets.
