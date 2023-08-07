from __future__ import annotations

import contextlib

import pytest

from pydiverse.pipedag import ConfigContext


@contextlib.contextmanager
def swallowing_raises(*args, **kwargs):
    with ConfigContext.get().evolve(swallow_exceptions=True):
        with pytest.raises(*args, **kwargs) as raises:
            yield raises
