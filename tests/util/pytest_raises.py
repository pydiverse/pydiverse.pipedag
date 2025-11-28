# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import contextlib

import pytest

from pydiverse.pipedag import ConfigContext


@contextlib.contextmanager
def swallowing_raises(*args, **kwargs):
    """Swallow logger.error messages and expect an exception to be raised."""
    # Attention: The option name `swallow_exceptions` is a bit misleading. It does not
    #   swallow the exception, but it swallows the error message that occurs between
    #   catching and rethrowing the exception and turns it into an info message.
    with ConfigContext.get().evolve(swallow_exceptions=True):
        with pytest.raises(*args, **kwargs) as raises:
            yield raises
