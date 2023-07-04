from __future__ import annotations

from pydiverse.pipedag.errors import DisposedError


class Disposable:
    def __getattribute__(self, name):
        try:
            object.__getattribute__(self, "_Disposable__disposed")
            obj_type = object.__getattribute__(self, "__class__")
            raise DisposedError(f"Object of type {obj_type} has already been disposed.")
        except AttributeError:
            pass

        return object.__getattribute__(self, name)

    def __setattr__(self, key, value):
        try:
            object.__getattribute__(self, "_Disposable__disposed")
            obj_type = object.__getattribute__(self, "__class__")
            raise DisposedError(f"Object of type {obj_type} has already been disposed.")
        except AttributeError:
            pass

        return object.__setattr__(self, key, value)

    def dispose(self):
        object.__setattr__(self, "_Disposable__disposed", True)
