# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import pytest

from tests.fixtures.instances import with_instances

try:
    import sqlalchemy as sa

    # this works only for SQLAlchemy >= 2.0 but is needed for example pipelines
    from sqlalchemy import Alias

    _ = Alias
except ImportError:
    sa = None

try:
    import dataframely as dy
except ImportError:
    dy = None

try:
    import pydiverse.transform as pdt
except ImportError:
    pdt = None

try:
    import pydiverse.colspec as cs
except ImportError:
    cs = None

try:
    import xgboost as xgb
except ImportError:
    xgb = None

examples = []
s3_examples = []
mssql_examples = []
ibmdb_examples = []

if sa:
    from example.run_pipeline import main as example_main
    from example.simple_pipeline import main as simple_pipeline_main
    from example.visualization import main as visualization_main
    from example_imperative.run_pipeline import main as example_imperative_main
    from example_interactive.run_tasks_interactively import main as example_interactive_main
    from example_postgres.run_pipeline_small import main as example_postgres_small

    examples = [
        example_main,
        simple_pipeline_main,
        visualization_main,
        example_imperative_main,
        example_interactive_main,
        example_postgres_small,
    ]

if sa and dy and cs and pdt:
    from example_ibm_db2.run_pipeline import main as example_ibmdb_main
    from example_mssql.run_pipeline import main as example_mssql_main
    from example_parquet_s3.run_pipeline import main as example_s3_main
    from example_postgres.run_pipeline import main as example_postgres_main

    examples.append(example_postgres_main)
    s3_examples.append(example_s3_main)
    mssql_examples.append(example_mssql_main)
    ibmdb_examples.append(example_ibmdb_main)

if sa and cs and pdt and xgb:
    from example_mssql.realistic_pipeline import main as example_mssql_realistic
    from example_parquet_s3.realistic_pipeline import main as example_s3_realistic
    from example_postgres.realistic_pipeline import main as example_postgres_realistic

    examples.append(example_postgres_realistic)
    s3_examples.append(example_s3_realistic)
    mssql_examples.append(example_mssql_realistic)


@pytest.mark.parametrize(
    "fn",
    examples,
)
@with_instances("postgres")
def test_examples_postgres(fn):
    """
    This test just runs the example pipeline that we provide in example/run_pipeline.py
    """

    fn()


@pytest.mark.parametrize(
    "fn",
    s3_examples,
)
@with_instances("parquet_s3_backend")
def test_examples_s3(fn):
    """
    This test just runs the example pipeline that we provide in example/run_pipeline.py
    """

    fn()


@pytest.mark.parametrize(
    "fn",
    mssql_examples,
)
@with_instances("mssql")
def test_examples_mssql(fn):
    """
    This test just runs the example pipeline that we provide in example/run_pipeline.py
    """

    fn()


@pytest.mark.parametrize(
    "fn",
    ibmdb_examples,
)
@with_instances("ibm_db2")
def test_examples_ibmdb(fn):
    """
    This test just runs the example pipeline that we provide in example/run_pipeline.py
    """

    fn()
