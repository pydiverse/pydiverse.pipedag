# Pixi env comparison: default vs py313all (parquet/GCS-relevant)

Comparison of `pixi list` vs `pixi list -e py313all` for packages relevant to parquet and GCS access.

## Summary

| Package | default | py313all | Note |
|---------|---------|----------|------|
| **Python** | 3.12.12 | 3.13.11 | Different interpreter |
| **fsspec** | **2025.12.0** | 2025.10.0 | default has newer fsspec |
| **gcsfs** | *(not in conda list)* | 2025.10.0 | py313all has gcsfs from conda; default may get it via pip |
| **google-auth** | *(not in conda list)* | 2.43.0 | py313all has it from conda |
| **google-cloud-storage** | *(not in conda list)* | 3.7.0 | py313all has it from conda |
| **google-auth-oauthlib** | *(not in conda list)* | 1.2.2 | py313all only |
| **google-api-core** | *(not in conda list)* | 2.28.1 | py313all only |
| **duckdb** | 1.4.3 | 1.4.3 | Same |
| **python-duckdb** | 1.4.3 | 1.4.3 | Same |
| **pyarrow** | 22.0.0 | 22.0.0 | Same |
| **pandas** | 2.3.3 | 2.3.3 | Same |
| **polars** | 1.36.1 | 1.36.1 | Same |
| **requests** | 2.32.5 | 2.32.5 | Same |
| **libgoogle-cloud** | 2.39.0 | 2.39.0 | Same (C++ lib) |
| **libgoogle-cloud-storage** | 2.39.0 | 2.39.0 | Same (C++ lib) |

## Takeaways

1. **py313all** pins an older **fsspec** (2025.10.0) and includes **gcsfs** and the full **google-*** Python stack from conda-forge. That can lead to different behaviour (e.g. the `'str' object is not callable` with impersonated credentials comes from the google-auth + gcsfs path used in py313all).

2. **default** uses a **newer fsspec** (2025.12.0) and does not list gcsfs/google-auth/google-cloud-storage in `pixi list`; those may be installed via pip (e.g. `[project].optional-dependencies` or test deps), so versions can differ.

3. For **reliable GCS + parquet** across envs, align:
   - **fsspec** and **gcsfs** versions (and prefer a version known to work with your auth, e.g. 2025.12.x if it fixes the impersonation refresh bug),
   - **google-auth** (and optionally **google-auth-oauthlib**, **google-cloud-storage**) to the same source (conda vs pip) and version in both default and py313all.

4. The **'str' object is not callable** was a **google-auth** bug:
   - In google-auth **< 2.48.0**, `google.auth.impersonated_credentials._refresh_token()` called `self._source_credentials._refresh_token(request)`.
   - But `google.oauth2.credentials.Credentials.__init__` stores `self._refresh_token = refresh_token` (a string), shadowing the base class method.
   - In google-auth **>= 2.48.0**, the method was renamed to `_perform_refresh_token` and calls `self._source_credentials.refresh(request)` instead, avoiding the clash entirely.
   - **Fix:** Pin `google-auth >= 2.48.0` in the s3 feature of `pixi.toml`. This resolves the issue for all envs (py312all, py313all, etc.).

## How to regenerate this comparison

```bash
pixi list | sort > /tmp/default.txt
pixi list -e py313all | sort > /tmp/py313all.txt
diff /tmp/default.txt /tmp/py313all.txt
```

Or search for specific packages:

```bash
pixi list | grep -E 'fsspec|gcsfs|google|duckdb|pyarrow|pandas|polars'
pixi list -e py313all | grep -E 'fsspec|gcsfs|google|duckdb|pyarrow|pandas|polars'
```
