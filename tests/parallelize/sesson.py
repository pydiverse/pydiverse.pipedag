from __future__ import annotations

import os
from multiprocessing import Process, Queue

from _pytest.python import Config

from .util import parse_config
from .worker import start_worker


class Session:
    def __init__(self, config: Config):
        self.config = config
        self.msg_queue = Queue()
        self.work_queue = Queue()

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
            raise ValueError('workers can only be an integer or "auto"')

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

        return groupings[0]

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

        # Prepare work queue
        grouped_items = self.get_grouped_items(session)
        for group, items in grouped_items.items():
            node_ids = [item.nodeid for item in items]
            self.work_queue.put(("GROUP", (group, node_ids)))

        num_workers = min(len(grouped_items), self.num_workers)
        for _ in range(num_workers):
            self.work_queue.put(("STOP", ()))

        args = self.config.args
        option_dict = vars(self.config.option)

        # Run Workers
        workers = []
        for worker_id in range(num_workers):
            worker = Process(
                target=start_worker,
                args=(
                    worker_id,
                    self.work_queue,
                    self.msg_queue,
                    args,
                    option_dict,
                ),
                daemon=True,
            )
            workers.append(worker)
            worker.start()

        # Wait for messages
        alive_workers = len(workers)
        while alive_workers:
            msg, kwargs = self.msg_queue.get()
            worker = workers[kwargs["worker_id"]]

            if msg == "sessionstart":
                pass
            elif msg == "sessionfinish":
                alive_workers -= 1
                worker.join()
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
            else:
                error_msg = f"Unknown message: {msg}"
                raise ValueError(error_msg)

        return True
