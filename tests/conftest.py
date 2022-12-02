import logging
import os
import sys
from pathlib import Path

import structlog

_log_level = logging.INFO
logging.basicConfig(
    stream=sys.stderr,
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=_log_level,
)
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(_log_level),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(sys.stderr),
    cache_logger_on_first_use=True,
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
