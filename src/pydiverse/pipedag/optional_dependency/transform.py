# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import types

try:
    import pydiverse.transform as pdt
    from pydiverse.transform import C, ColExpr, Pandas, Polars, SqlAlchemy, verb
except ImportError:

    def verb(func):
        """A no-op decorator for functions that are intended to be used as verbs."""
        return func

    class Table:
        pass

    class ColExpr:
        pass

    # Create a new module with the given name.
    pdt = types.ModuleType("pydiverse.transform")
    pdt.Table = Table
    pdt.SqlAlchemy = object
    pdt.Polars = object
    pdt.Pandas = object
    C = None
    SqlAlchemy = object
    Polars = object
    Pandas = object

__all__ = ["pdt", "pdt_old", "pdt_new", "C", "ColExpr", "Pandas", "Polars", "SqlAlchemy", "verb"]
