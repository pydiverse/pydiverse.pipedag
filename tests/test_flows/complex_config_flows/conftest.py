import os
from pathlib import Path

import pytest

os.environ["POSTGRES_PASSWORD_CFG"] = str(
    Path(__file__).parent / "postgres_password.yaml"
)


@pytest.fixture(scope="session", params=["pipedag_complex.yaml", "pipedag_anchor.yaml"])
def cfg_file_path(request):
    return Path(__file__).parent / request.param
