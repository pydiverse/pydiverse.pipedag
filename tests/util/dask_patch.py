# Patch pytest EncodedFile (from pytest-capture plugin) to be pickleable
# https://github.com/mariusvniekerk/pytest-dask/blob/master/pytest_dask/serde_patch.py
from io import BytesIO

from _pytest.capture import EncodedFile


def apply_getsetstate(cls):
    def inner(ref):
        cls.__getstate__ = ref.__getstate__
        cls.__setstate__ = ref.__setstate__

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

    def __setstate__(self, state):
        assert isinstance(self, EncodedFile)
        self.encoding = state["encoding"]
        buffer = BytesIO(state["value"])
        self.buffer = buffer
