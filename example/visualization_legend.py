from __future__ import annotations

import tempfile

from pydiverse.pipedag import Flow, GroupNode, Stage, VisualizationStyle, materialize
from pydiverse.pipedag.core.config import create_basic_pipedag_config
from pydiverse.pipedag.util.structlog import setup_logging


@materialize
def failed():
    raise AssertionError("This task is supposed to fail")


@materialize(version=None)
def completed_but_cache_invalid():
    return 1


@materialize(version="1.0")
def cache_valid():
    return 2


@materialize(version="1.0")
def cache_valid2():
    return 3


@materialize
def skipped(out):
    return out + 1


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg = (
            create_basic_pipedag_config(
                f"duckdb:///{temp_dir}/db.duckdb",
                disable_stage_locking=True,  # This is special for duckdb
                # Attention: stage and task names might be sent to the
                #   following URL. You can self-host kroki if you like:
                #   https://docs.kroki.io/kroki/setup/install/
                kroki_url="https://kroki.io",
                fail_fast=False,
            )
            .get("default")
            .evolve(swallow_exceptions=True)
        )
        with cfg:
            with Flow() as flow:
                with Stage("stage1"):
                    _ = completed_but_cache_invalid()
                    _ = cache_valid()
                with Stage("stage2"):
                    out = failed()
                with Stage("stage3"):
                    _ = skipped(out)
                    with GroupNode(
                        "group_none_cache_valid",
                        style=VisualizationStyle(hide_content=True),
                    ):
                        _ = completed_but_cache_invalid()
                    with GroupNode(
                        "group_any_cache_valid",
                        style=VisualizationStyle(hide_content=True),
                    ):
                        _ = completed_but_cache_invalid()
                        _ = cache_valid()
                    with GroupNode(
                        "group_all_cache_valid",
                        style=VisualizationStyle(hide_content=True),
                    ):
                        # avoid memoization (not counted as cache valid)
                        _ = cache_valid2()
                    with GroupNode(
                        "group_any_failed", style=VisualizationStyle(hide_content=True)
                    ):
                        _ = completed_but_cache_invalid()
                        out = failed()
                    with GroupNode(
                        "group_all_skipped", style=VisualizationStyle(hide_content=True)
                    ):
                        _ = skipped(out)

            # Run flow
            result = flow.run()
            assert not result.successful

            # Run flow again for cache validity
            result = flow.run()
            assert not result.successful

            # you can also visualize the flow explicitly:
            # kroki_url = result.visualize_url()
            # result.visualize()


if __name__ == "__main__":
    setup_logging()  # you can setup the logging and/or structlog libraries as you wish
    main()
