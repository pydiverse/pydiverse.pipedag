from __future__ import annotations

import io
import pickle
from io import BytesIO

import dask
import structlog
from _pytest.capture import EncodedFile


class A(io.TextIOWrapper):
    def __getstate__(self):
        return "a"

    def __reduce__(self):
        return A, (BytesIO(b"hello"),)

    def __reduce_ex__(self, protocol):
        _ = protocol
        return self.__reduce__()


def test_that_io_wrapper_is_pickleable():
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
