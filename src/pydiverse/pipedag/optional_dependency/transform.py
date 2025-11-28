# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import types

try:
    import pydiverse.transform as pdt
    from pydiverse.transform import C, ColExpr, Pandas, Polars, SqlAlchemy, verb

    try:
        from pydiverse.transform.eager import PandasTableImpl

        _ = PandasTableImpl

        pdt_old = pdt
        pdt_new = None
    except ImportError:
        try:
            # detect if 0.2 or >0.2 is active
            # this import would only work in <=0.2
            from pydiverse.transform.extended import Polars

            # ensures a "used" state for the import, preventing black from deleting it
            _ = Polars

            pdt_old = None
            pdt_new = pdt
        except ImportError:
            raise NotImplementedError("pydiverse.transform 0.2.0 - 0.2.2 isn't supported") from None
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
    pdt_old = None
    pdt_new = None
    SqlAlchemy = object
    Polars = object
    Pandas = object
