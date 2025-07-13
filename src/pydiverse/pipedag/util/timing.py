# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import signal
from contextlib import contextmanager


@contextmanager
def timeout(seconds=1, error_message="timeout reached"):
    """
    Context manager to raise a TimeoutError after a specified number of seconds.
    """

    def alarm_handler(signum, frame):
        _ = signum, frame
        raise TimeoutError(error_message)

    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(seconds)

    try:
        yield
    finally:
        signal.alarm(0)
