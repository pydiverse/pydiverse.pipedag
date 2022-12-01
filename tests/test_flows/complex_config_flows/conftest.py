import os
from pathlib import Path

import pytest
import structlog
from _pytest.fixtures import fixture

_logger = structlog.getLogger(module=__name__)
_logger.info("Default config file might be ignored for the following tests")

os.environ["POSTGRES_PASSWORD_CFG"] = str(
    Path(__file__).parent / "postgres_password.yaml"
)


@fixture(scope="session", params=["pipedag_complex", "pipedag_anchor"])
def cfg_file_base_name(base_name):
    return cfg_file_base_name
