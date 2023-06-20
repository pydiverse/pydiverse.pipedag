from __future__ import annotations

import copy
import unittest.mock

from pydiverse.pipedag.core.task import Task, TaskGetItem
from pydiverse.pipedag.materialize.core import MaterializingTask


class PipedagMock:
    def __init__(self, mock: unittest.mock.Mock):
        self.mock = mock
        self._last_call_count = mock.call_count

    def reset_call_count(self):
        self._last_call_count = self.mock.call_count

    def _calls_since_last_time(self):
        delta = self.mock.call_count - self._last_call_count
        self.reset_call_count()
        return delta

    def _assert_call_count(self, n):
        __tracebackhide__ = True
        m = self._calls_since_last_time()
        if n == m:
            return
        name = self.mock.mock.__dict__["_mock_name"]
        msg = (
            f"Expected function '{name}' to have been called {n} times, but it has"
            f" been called {m} times ({self.mock.call_count} times in total)."
        )
        raise AssertionError(msg)

    def assert_not_called(self):
        __tracebackhide__ = True
        self._assert_call_count(0)

    def assert_called_once(self):
        __tracebackhide__ = True
        self._assert_call_count(1)

    def assert_called(self, times):
        __tracebackhide__ = True
        self._assert_call_count(times)


def spy_task(mocker, task) -> PipedagMock:
    if isinstance(task, TaskGetItem):
        task = task.task
    if isinstance(task, MaterializingTask):
        task.fn = copy.copy(task.fn)
        spy = mocker.spy(task.fn, "fn")
    elif isinstance(task, Task):
        task_fn = task.fn

        def fn(*args, **kwargs):
            return task_fn(*args, **kwargs)

        task.fn = fn
        spy = mocker.spy(task, "fn")
    else:
        raise TypeError("Expected object of type Task or TaskGetItem")

    spy.mock.__dict__["_mock_name"] = task.name
    return PipedagMock(spy)
