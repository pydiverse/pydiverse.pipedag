from __future__ import annotations

from itertools import chain

import pytest

from pydiverse.pipedag import PipedagConfig

__all__ = [
    "DATABASE_INSTANCES",
    "ORCHESTRATION_INSTANCES",
    "ALL_INSTANCES",
    "with_instances",
    "skip_instances",
]


# Pytest markers associated with specific instance name
INSTANCE_MARKS = {
    # Database Instances
    "postgres": pytest.mark.postgres,
    "postgres_unlogged": pytest.mark.postgres,
    "mssql": pytest.mark.mssql,
    "mssql_pytsql": pytest.mark.mssql,
    "ibm_db2": pytest.mark.ibm_db2,
    "ibm_db2_avoid_schema": pytest.mark.ibm_db2,
    "ibm_db2_materialization_details": pytest.mark.ibm_db2,
    "duckdb": pytest.mark.duckdb,
    "snowflake": pytest.mark.snowflake,
    # Local Table Cache Instances
    "local_table_cache": pytest.mark.postgres,
    "local_table_cache_inout": pytest.mark.postgres,
    "local_table_cache_inout_numpy": pytest.mark.postgres,
    "local_table_store": pytest.mark.postgres,
    # Orchestration Instances
    "dask_engine": [pytest.mark.dask, pytest.mark.postgres],
    "prefect_engine": [pytest.mark.prefect, pytest.mark.postgres],
}

# Collection of instances that represent different database technologies
DATABASE_INSTANCES = (
    "postgres",
    "mssql",
    "ibm_db2",
    "duckdb",
)

ORCHESTRATION_INSTANCES = (
    "dask_engine",
    "prefect_engine",
)

# Extended collection of instances
ALL_INSTANCES = (
    "postgres",
    "postgres_unlogged",
    "mssql",
    "mssql_pytsql",
    "ibm_db2",
    "ibm_db2_avoid_schema",
    "ibm_db2_materialization_details",
    "duckdb",
    "snowflake",
    "local_table_cache",
)


def with_instances(*instances, **kwargs):
    """Decorator to run a test with a specific set of instances

    :param instances: Names of the instances to use.
    :param kwargs: keyword arguments passed to PipedagConfig.default.get()
    """
    return pytest.mark.instances(*flatten(instances), **kwargs)


def skip_instances(*instances):
    """Decorator to skip running a test with a specific set of instances"""
    return pytest.mark.skip_instances(*flatten(instances))


def flatten(it):
    """Flatten an iterable"""
    if isinstance(it, (list, tuple)):
        yield from chain(*map(flatten, it))
    else:
        yield it


# FIXTURE IMPLEMENTATION


@pytest.fixture(autouse=True, scope="function", name="run_with_instance")
def fixture_run_with_instance(request):
    """Fixture that runs test with different config instances"""
    if hasattr(request, "param"):
        instance, kwargs = request.param
        config = PipedagConfig.default.get(instance=instance, **kwargs)
        with config:
            yield instance
    else:
        yield None
