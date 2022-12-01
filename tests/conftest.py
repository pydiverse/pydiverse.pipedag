import logging
import os
import sys
from pathlib import Path

import structlog

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
_cfg_file = "pipedag.yaml"
os.environ["PIPEDAG_CONFIG"] = str(Path(__file__).parent / _cfg_file)
os.environ["POSTGRES_USERNAME"] = "sa"
os.environ["POSTGRES_PASSWORD"] = "Pydiverse23"
os.environ["MSSQL_PASSWORD"] = "PidyQuant27"

_logger = structlog.getLogger(module=__name__)
_logger.info("Using default config file", file=_cfg_file)
