from __future__ import annotations

import itertools
import os
import signal
import time
from multiprocessing import Process, Queue
from queue import Empty
from threading import Thread

import pytest
from _pytest.python import Config

from .util import parse_config
from .worker import start_worker


class Session:
    def __init__(self, config: Config):
        self.config = config
        self.msg_queue = Queue()
        self.work_queue = Queue()

        self.workers = []
        self.worker_id_counter = itertools.count()
        self.running_workers = set()

        self.debug_worker_group = {}
        self.debug_worker_test = {}

        self._should_shutdown = False
        self._shutdown_reason = "None"

        # Hide path from output
        reporter = config.pluginmanager.getplugin("terminalreporter")
        reporter.showfspath = False

        # Get the number of workers
        try:
            num_workers = parse_config(config, "workers")
            if num_workers == "auto":
                num_workers = os.cpu_count() or 1
            elif num_workers is not None:
                num_workers = int(num_workers)
            else:
                num_workers = 1
            self.num_workers = num_workers
        except ValueError:
            raise ValueError('workers can only be an integer or "auto"') from None

        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def pytest_runtestloop(self, session):
        if (
            session.testsfailed
            and not session.config.option.continue_on_collection_errors
        ):
            raise session.Interrupted(
                "%d error%s during collection"
                % (session.testsfailed, "s" if session.testsfailed != 1 else "")
            )

        if session.config.option.collectonly:
            return True

        # Prepare everything
        grouped_items = self.get_grouped_items(session)
        num_workers = min(len(grouped_items), self.num_workers)

        self.prepare_work_queue(num_workers, grouped_items)
        self.start_workers(num_workers)

        # Monitor workers
        def check_workers_alive_loop():
            while True:
                self.check_workers_alive()
                time.sleep(1)

        thread = Thread(target=check_workers_alive_loop, daemon=True)
        thread.start()

        # Handle messages
        while self.running_workers and not self._should_shutdown:
            try:
                msg, kwargs = self.msg_queue.get(timeout=1)
            except Empty:
                continue

            worker = self.workers[kwargs["worker_id"]]

            if msg == "sessionstart":
                pass
            elif msg == "sessionfinish":
                worker.join()
                self.running_workers.remove(worker)
            elif msg == "logstart":
                self.config.hook.pytest_runtest_logstart(
                    nodeid=kwargs["nodeid"], location=kwargs["location"]
                )
            elif msg == "logfinish":
                self.config.hook.pytest_runtest_logfinish(
                    nodeid=kwargs["nodeid"], location=kwargs["location"]
                )
            elif msg == "logreport":
                report = self.config.hook.pytest_report_from_serializable(
                    config=self.config, data=kwargs["report"]
                )
                self.config.hook.pytest_runtest_logreport(report=report)
            elif msg == "DEBUG_start_group":
                self.debug_worker_group[worker] = kwargs["group_name"]
            elif msg == "DEBUG_start_test":
                self.debug_worker_test[worker] = kwargs["nodeid"]
            else:
                error_msg = f"Unknown message: {msg}"
                raise ValueError(error_msg)

        if self._should_shutdown:
            pytest.exit(self._shutdown_reason)

        return True

    def get_grouped_items(self, session) -> dict[str, list]:
        groupings = self.config.hook.pytest_parallelize_group_items(
            config=self.config,
            items=session.items,
        )

        if len(groupings) == 0:
            msg = "No implementation for `pytest_parallelize_group_items` hook found."
            raise AssertionError(msg)
        if len(groupings) > 1:
            msg = (
                "Multiple implementations for `pytest_parallelize_group_items` hook"
                " found."
            )
            raise AssertionError(msg)

        grouping = groupings[0]

        reporter = session.config.pluginmanager.getplugin("terminalreporter")
        if reporter.showheader and not reporter.no_header:
            reporter.line(
                f"Split tests into {len(grouping)} parallelization groups: "
                + ", ".join(sorted(list(grouping)))
            )

        return grouping

    def prepare_work_queue(self, num_workers, grouped_items):
        for group, items in grouped_items.items():
            node_ids = [item.nodeid for item in items]
            self.work_queue.put(("GROUP", (group, node_ids)))

        for _ in range(num_workers):
            self.work_queue.put(("STOP", ()))

    def start_workers(self, num_workers):
        args = self.config.args
        option_dict = vars(self.config.option)

        for _ in range(num_workers):
            worker_id = next(self.worker_id_counter)
            worker = Process(
                target=start_worker,
                name=f"pytest-worker-{worker_id:03}",
                args=(
                    worker_id,
                    self.work_queue,
                    self.msg_queue,
                    args,
                    option_dict,
                ),
                daemon=False,
            )
            worker.start()

            self.workers.append(worker)
            self.running_workers.add(worker)

    def check_workers_alive(self):
        for worker in self.running_workers:
            if not worker.is_alive():
                group_name = self.debug_worker_group.get(worker)
                test_name = self.debug_worker_test.get(worker)
                msg = (
                    f"Worker {worker.name} died with exit code {worker.exitcode}."
                    f" (group = {group_name}, test = {test_name})"
                )
                self.shutdown(msg)

    def shutdown(self, reason: str):
        self._shutdown_reason = reason
        self._should_shutdown = True

    def exit_gracefully(self, signum, frame):
        for worker in self.workers:
            worker.terminate()

        signame = signal.Signals(signum).name
        pytest.exit(f"Received signal {signame}")
