from __future__ import annotations

import logging
import sys
import textwrap
from io import StringIO

import structlog
from structlog.typing import EventDict, WrappedLogger


class StructlogHandler(logging.Handler):
    """
    Stdlib logging handler that feeds all events back into structlog

    Can't be used with a structlog logger_factory that uses the logging library,
    otherwise logging would result in an infinite loop.
    """

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._log = structlog.get_logger()

    def emit(self, record):
        msg = self.format(record)
        self._log.log(record.levelno, msg, logger=record.name)


class PipedagConsoleRenderer(structlog.dev.ConsoleRenderer):
    """
    Custom subclass of the structlog ConsoleRenderer that allows rendering
    specific values in the event dict on separate lines.
    """

    def __init__(self, *args, **kwargs):
        self._render_keys = kwargs.pop("render_keys", [])
        super().__init__(*args, **kwargs)

    def __call__(self, logger: WrappedLogger, name: str, event_dict: EventDict):
        render_objects = {}
        for key in self._render_keys:
            obj = event_dict.pop(key, None)
            if obj is not None:
                render_objects[key] = obj

        result = super().__call__(logger, name, event_dict)
        sio = StringIO()
        sio.write(result)

        for key, obj in render_objects.items():
            string_rep = str(obj)
            sio.write(
                "\n"
                + "    ["
                + self._styles.kv_key
                + key
                + self._styles.reset
                + "]"
                + "\n"
                + textwrap.indent(string_rep, prefix="    " + self._styles.kv_value)
                + self._styles.reset
            )

        return sio.getvalue()


def setup_logging(
    log_level=logging.INFO,
    log_stream=sys.stderr,
    timestamp_format="%Y-%m-%d %H:%M:%S.%f",
):
    """Configures structlog and logging with sane defaults."""

    # Redirect all logs submitted to logging to structlog
    logging.basicConfig(
        format="%(message)s",
        level=log_level,
        handlers=[StructlogHandler()],
    )

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(timestamp_format),
            PipedagConsoleRenderer(render_keys=["query", "table_obj"]),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(log_stream),
        cache_logger_on_first_use=True,
    )
