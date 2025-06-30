# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import tempfile

from pydiverse.common.util.structlog import setup_logging
from pydiverse.pipedag import Flow, GroupNode, Stage, VisualizationStyle, materialize
from pydiverse.pipedag.core.config import create_basic_pipedag_config


@materialize
def any_task():
    return 1


@materialize
def task_within_group():
    return 2


@materialize
def task_within_group2(input1: int):
    return input1 + 1


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg = create_basic_pipedag_config(
            f"duckdb:///{temp_dir}/db.duckdb",
            disable_stage_locking=True,  # This is special for duckdb
            # Attention: stage and task names might be sent to the
            #   following URL. You can self-host kroki if you like:
            #   https://docs.kroki.io/kroki/setup/install/
            kroki_url="https://kroki.io",
        ).get("default")
        with cfg:
            with Flow() as flow:
                with Stage("stage1"):
                    _ = any_task()
                    with GroupNode(
                        "group1",
                        ordering_barrier=True,
                        style=VisualizationStyle(hide_content=True, box_color_always="#ccccff"),
                    ):
                        task1 = task_within_group()
                        _ = task_within_group2(task1)
                    _ = any_task()

            # Run flow
            result = flow.run()
            assert result.successful

            # you can also visualize the flow explicitly:
            # kroki_url = result.visualize_url()
            # result.visualize()


if __name__ == "__main__":
    setup_logging()  # you can setup the logging and/or structlog libraries as you wish
    main()
