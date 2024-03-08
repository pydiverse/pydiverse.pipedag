from __future__ import annotations

from example.run_pipeline import main as example_flow_main
from example_postgres.run_pipeline import main as example_postgres_flow_main


def test_example_flow():
    """
    This test just runs the example pipeline that we provide in example/run_pipeline.py
    """

    example_flow_main()


def test_example_postgres_flow():
    """
    This test just runs the example pipeline that we provide in
    example_postgres/run_pipeline.py
    """

    example_postgres_flow_main()
