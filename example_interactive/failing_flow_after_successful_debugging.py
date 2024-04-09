from __future__ import annotations

import logging
import tempfile

import sqlalchemy as sa
from sqlalchemy.exc import ProgrammingError

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.core.config import create_basic_pipedag_config
from pydiverse.pipedag.util.structlog import setup_logging


@materialize(lazy=True)
def lazy_task_1():
    try:
        tbl = Table(sa.text("SELECT-TYPO 1"), name="tbl").materialize()
    except ProgrammingError:
        # This error is expected
        logger = logging.getLogger(__name__ + "-lazy_task_1")
        logger.info("Caught expected error", exc_info=True)

    # now we succeed, but are still not done, yet
    tbl = Table(sa.text("SELECT 'not-done-yet' as a"), name="tbl").materialize()

    # this will create another two tables but they are not returned and won't switch to
    # debug mode
    Table(sa.text("SELECT 3 as a")).materialize()
    Table(sa.text("SELECT 4 as a"), name="tbl2").materialize()

    # now, we succeed with fixing `tbl` and automatically switch in debug mode
    tbl = Table(sa.text("SELECT 1 as a"), name="tbl").materialize()

    # we can also keep a table object:
    tbl_obj = Table(sa.text("SELECT 'not-done-yet' as a"))
    tbl_obj.materialize()

    # this will also automatically switch to debug mode
    tbl_obj.obj = sa.text("SELECT 1 as a")
    tbl_obj.materialize()

    # However, now the flow will stop because cache invalidation cannot deal with debug
    # mode
    return tbl


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg = create_basic_pipedag_config(
            f"duckdb:///{temp_dir}/db.duckdb",
            disable_stage_locking=True,  # This is special for duckdb
            # Attention: If uncommented, stage and task names might be sent to the
            #   following URL. You can self-host kroki if you like:
            #   https://docs.kroki.io/kroki/setup/install/
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
