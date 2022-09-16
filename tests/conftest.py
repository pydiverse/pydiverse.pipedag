import logging
import os

import structlog

os.environ["PIPEDAG_CONFIG"] = os.path.join(os.path.dirname(__file__), "pipedag.toml")

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
