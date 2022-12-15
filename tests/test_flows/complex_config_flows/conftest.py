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


@fixture(scope="session", params=["pipedag_complex.yaml", "pipedag_anchor.yaml"])
def cfg_file_path(request):
    return Path(__file__).parent / request.param
