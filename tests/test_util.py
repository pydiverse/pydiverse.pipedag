from __future__ import annotations

import traceback

import pytest

from pydiverse.pipedag.errors import DisposedError
from pydiverse.pipedag.util import Disposable, requires


def test_requires():
    @requires(None, ImportError("Some Error"))
    class BadClass:
        a = 1
        b = 2

    # Shouldn't be able to create instance
    with pytest.raises(ImportError, match="Some Error"):
        BadClass()

    # Shouldn't be able to access class attribute
    with pytest.raises(ImportError, match="Some Error"):
        _ = BadClass.a

    # If all requirements are fulfilled, nothing should change
    @requires((pytest,), Exception("This shouldn't happen"))
    class GoodClass:
        a = 1

    _ = GoodClass()
    _ = GoodClass.a


def test_disposable():
    class Foo(Disposable):
        a = 1

        def bar(self):
            return 2

    x = Foo()

    assert x.a == 1
    assert x.bar() == 2

    x.dispose()

    with pytest.raises(DisposedError):
        _ = x.a
    with pytest.raises(DisposedError):
        x.foo()
    with pytest.raises(DisposedError):
        x.dispose()
    with pytest.raises(DisposedError):
        x.a = 1


def test_format_exception():
    # traceback.format_exception syntax changed from python 3.9 to 3.10
    # thus we use traceback.format_exc()
    try:
        raise RuntimeError("this error is intended by test")
    except RuntimeError:
        trace = traceback.format_exc()
        assert 'RuntimeError("this error is intended by test")' in trace
        assert "test_util.py" in trace
