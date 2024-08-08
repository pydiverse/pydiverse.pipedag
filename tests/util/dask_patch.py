from __future__ import annotations

from io import BytesIO

# Patch pytest EncodedFile (from pytest-capture plugin) to be pickleable
# https://github.com/mariusvniekerk/pytest-dask/blob/master/pytest_dask/serde_patch.py
from _pytest.capture import EncodedFile


def apply_getsetstate(cls):
    def inner(ref):
        cls.__getstate__ = ref.__getstate__
        cls.__reduce__ = ref.__reduce__
        cls.__reduce_ex__ = ref.__reduce_ex__
        return cls

    return inner


@apply_getsetstate(EncodedFile)
class _EncodedFile:
    def __getstate__(self):
        assert isinstance(self, EncodedFile)
        current_position = self.buffer.seek(0, 1)
        self.buffer.seek(0)
        value = self.buffer.read()
        self.buffer.seek(current_position, 0)
        return {"value": value, "encoding": self.encoding}

    def __reduce__(self):
        state = self.__getstate__()
        return self.__class__, (BytesIO(state["value"]), state["encoding"])

    def __reduce_ex__(self, protocol):
        _ = protocol
        return self.__reduce__()
