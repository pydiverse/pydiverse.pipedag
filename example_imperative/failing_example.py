# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import tempfile

import sqlalchemy as sa

from pydiverse.common.util.structlog import setup_logging
from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.core.config import create_basic_pipedag_config


@materialize(lazy=True)
def lazy_task_1():
    return Table(sa.text("<invalid query>")).materialize()


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg = create_basic_pipedag_config(
            f"duckdb:///{temp_dir}/db.duckdb",
            disable_stage_locking=True,  # This is special for duckdb
            # Attention: If uncommented, stage and task names might be sent to the
            #   following URL. You can self-host kroki if you like:
            #   https://docs.kroki.io/kroki/setup/install/
            #   You need to install optional dependency 'pydot' for any visualization
            #   URL to appear.
            # kroki_url="https://kroki.io",
        ).get("default")
        with cfg:
            with Flow() as f:
                with Stage("stage_1"):
                    lazy_task_1()

            # Run flow
            f.run()


if __name__ == "__main__":
    setup_logging()  # you can setup the logging and/or structlog libraries as you wish
    main()
