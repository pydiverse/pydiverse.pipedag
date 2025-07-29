# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import pytest

from example.run_pipeline import main as example_flow_main
from example.simple_pipeline import main as simple_pipeline_main
from example.visualization import main as visualization_main
from example_imperative.run_pipeline import main as example_imperative_flow_main
from example_interactive.run_tasks_interactively import main as example_interactive_main
from example_parquet_s3.run_pipeline import main as example_s3_main
from example_postgres.run_pipeline import main as example_postgres_flow_main
from tests.fixtures.instances import with_instances


@pytest.mark.parametrize(
    "fn",
    [
        example_flow_main,
        simple_pipeline_main,
        visualization_main,
        example_imperative_flow_main,
        example_postgres_flow_main,
        example_interactive_main,
    ],
)
@with_instances("postgres")
def test_examples(fn):
    """
    This test just runs the example pipeline that we provide in example/run_pipeline.py
    """

    fn()


@pytest.mark.parametrize(
    "fn",
    [
        example_s3_main,
    ],
)
@with_instances("parquet_s3_backend")
def test_examples_s3(fn):
    """
    This test just runs the example pipeline that we provide in example/run_pipeline.py
    """

    fn()
