# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

# pipedag supports both sqlalchemy < 2 and >= 2

try:
    from sqlalchemy import URL, Alias, Connection, Engine, Select, TextClause
    from sqlalchemy import Text as SqlText
except ImportError:
    # For compatibility with sqlalchemy < 2.0
    from sqlalchemy.engine import URL, Connection, Engine
    from sqlalchemy.sql.expression import Alias, Select, TextClause

    SqlText = TextClause  # this is what sa.text() returns


__all__ = [
    "URL",
    "Alias",
    "Connection",
    "Engine",
    "Select",
    "SqlText",
    "TextClause",
]
