from __future__ import annotations

import io
import pickle
import sys
from io import BytesIO

import dask
import pytest
import structlog
from _pytest.capture import EncodedFile

# Attention: Running dask tests within pytest (without option -s) fails on python 3.12
# because dask_patch.py cannot successfully make EncodedFile pickleable by adding
# __getstate__ and __setstate__ methods. Let's see what the future of python brings.


class A(io.TextIOWrapper):
    def __getstate__(self):
        return "a"

    def __reduce__(self):
        return None

    def __reduce_ex__(self, protocol):
        _ = protocol
        return None


def test_that_io_wrapper_is_pickleable():
    if sys.version_info >= (3, 12):
        with pytest.raises(TypeError, match="cannot pickle 'A' instances"):
            pickle.dumps(A(BytesIO(b"hello")))
    else:
        pickle.dumps(A(BytesIO(b"hello")))


def test_that_encoded_file_is_picklable():
    pickle.dumps(EncodedFile(BytesIO(b"hello"), "utf-8"))


def test_dask_structlog_configuration_does_not_prevent_pickling():
    def bind_run():
        structlog_config = structlog.get_config()

        def run(parent_futures, **kwargs):
            _ = parent_futures

            structlog.configure(**structlog_config)

            return 1

        run.__name__ = "hi"
        return dask.delayed(run, pure=False)

    results = [bind_run()(parent_futures=[])]
    kw = {
        "traverse": True,
        "optimize_graph": False,
        "scheduler": "processes",
        "num_workers": 8,
        "chunksize": 1,
    }

    dask.compute(results, **kw)
