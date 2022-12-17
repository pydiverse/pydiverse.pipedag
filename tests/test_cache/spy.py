from __future__ import annotations

import copy
import unittest.mock

from pydiverse.pipedag.core.task import TaskGetItem
from pydiverse.pipedag.materialize.core import MaterializingTask


def spy_task(mocker, task) -> unittest.mock.Mock:
    if isinstance(task, TaskGetItem):
        task = task.task
    if not isinstance(task, MaterializingTask):
        raise TypeError("Expected object of type MaterializingTask or TaskGetItem")

    task.fn = copy.copy(task.fn)
    spy = mocker.spy(task.fn, "fn")
    spy.mock.__dict__["_mock_name"] = task.name
    return spy
