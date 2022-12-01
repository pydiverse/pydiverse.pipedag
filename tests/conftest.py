import logging
import os
import sys
from pathlib import Path

import structlog

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
os.environ["PIPEDAG_CONFIG"] = str(Path(__file__).parent)
os.environ["POSTGRES_USERNAME"] = "sa"
os.environ["POSTGRES_PASSWORD"] = "Pydiverse23"
os.environ["MSSQL_PASSWORD"] = "PidyQuant27"

_logger = structlog.getLogger(module=__name__)
_logger.info(
    "Setting default config directory via PIPEDAG_CONFIG variable",
    dir=os.environ["PIPEDAG_CONFIG"],
)
