from __future__ import annotations

from abc import ABC
from multiprocessing import Queue

import pytest
from _pytest.config import Config


def start_worker(
    worker_id: int, work_queue: Queue, msg_queue: Queue, args: list, option_dict: dict
):
    option_dict["plugins"].append("no:terminal")
    config = Config.fromdictargs(option_dict, args)
    config.args = args

    from typing import TextIO

    class DontPrint(TextIO, ABC):
        def write(*_):
            pass

    # TODO: find a way to fix assert inspection code of pytest raised in threads
    # The following code meant to do this, but prevents tests from running at all.
    # # register dummy terminal reporter since it is needed by pytest even with
    # # plugins:"no:terminal" option
    # terminal_reporter = TerminalReporter(config, DontPrint())
    # config.pluginmanager.register(terminal_reporter, "terminalreporter")

    # Remove workers option to prevent triggering main plugin
    config.option.workers = None

    worker = Worker(config, worker_id, work_queue, msg_queue)
    config.pluginmanager.register(worker)
    config.hook.pytest_cmdline_main(config=config)


class Worker:
    def __init__(self, config, worker_id: int, work_queue: Queue, msg_queue: Queue):
        super().__init__()

        self.config = config
        self.worker_id = worker_id
        self.work_queue = work_queue
        self.msg_queue = msg_queue

        self.session_items = {}

    def send(self, msg, **kwargs):
        kwargs["worker_id"] = self.worker_id
        self.msg_queue.put((msg, kwargs))

    @pytest.hookimpl
    def pytest_sessionstart(self, session):
        self.send("sessionstart")

    @pytest.hookimpl
    def pytest_sessionfinish(self, session):
        self.send("sessionfinish")

    @pytest.hookimpl
    def pytest_runtest_logstart(self, nodeid, location):
        self.send("logstart", nodeid=nodeid, location=location)

    @pytest.hookimpl
    def pytest_runtest_logfinish(self, nodeid, location):
        self.send("logfinish", nodeid=nodeid, location=location)

    @pytest.hookimpl
    def pytest_runtest_logreport(self, report):
        data = self.config.hook.pytest_report_to_serializable(
            config=self.config,
            report=report,
        )
        self.send("logreport", report=data)

    @pytest.hookimpl
    def pytest_runtestloop(self, session):
        self.session_items = {item.nodeid: item for item in session.items}

        should_terminate = False
        while not should_terminate:
            command, args = self.work_queue.get()
            should_terminate = self.process_one_item(session, command, args)
        return True

    def process_one_item(self, session, command, args):
        if command == "STOP":
            return True

        if command == "GROUP":
            group_name, node_ids = args
            items = [self.session_items[node_id] for node_id in node_ids]

            self.send("DEBUG_start_group", group_name=group_name)
            for i, item in enumerate(items):
                next_item = items[i + 1] if i + 1 < len(items) else None
                self.run_one_test(session, item, next_item)

        return False

    def run_one_test(self, session, item, next_item):
        self.send("DEBUG_start_test", nodeid=item.nodeid)
        item.ihook.pytest_runtest_protocol(item=item, nextitem=next_item)
        if session.shouldfail:
            raise session.Failed(session.shouldfail)
        if session.shouldstop:
            raise session.Interrupted(session.shouldstop)
