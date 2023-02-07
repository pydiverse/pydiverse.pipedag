import pytest

from pydiverse.pipedag.util import PipedagConfig


__all__ = [
    "instance",
    "skip_instance",
]


@pytest.fixture(
    autouse=True,
    scope="function",
    params=[
        pytest.param("postgres", marks=pytest.mark.postgres),
        pytest.param("mssql", marks=pytest.mark.mssql),
        pytest.param("mssql_pytsql", marks=pytest.mark.mssql),
        pytest.param("ibm_db2", marks=pytest.mark.ibm_db2),
        pytest.param("ibm_db2_avoid_schema", marks=pytest.mark.ibm_db2),
    ],
)
def instance(request):
    """Fixture that runs test with different config instances"""
    instance = request.param
    config = PipedagConfig.default.get(instance=instance)
    with config:
        yield instance


@pytest.fixture(autouse=True)
def skip_instance(request, instance):
    """Fixture that allows skipping a specific instance"""
    if marker := request.node.get_closest_marker("skip_instance"):
        if instance in marker.args:
            pytest.skip(f"instance '{instance}' is not supported.")
