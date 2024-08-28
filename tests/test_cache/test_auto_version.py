from __future__ import annotations

import pandas as pd
import pytest

from pydiverse.pipedag import AUTO_VERSION, Blob, Flow, Stage, Table
from pydiverse.pipedag.container import RawSql
from pydiverse.pipedag.materialize.core import materialize
from tests.fixtures.instances import with_instances
from tests.util import swallowing_raises

pytestmark = [with_instances("postgres"), with_instances("local_table_store")]


# Specific backends have tests in the test_table_hooks folder


def test_lazy_incompatible_with_auto_version():
    with pytest.raises(ValueError):

        @materialize(input_type=pd.DataFrame, version=AUTO_VERSION, lazy=True)
        def task():
            ...


def test_missing_input_type_auto_version():
    with pytest.raises(ValueError):

        @materialize(version=AUTO_VERSION)
        def task():
            ...


@with_instances("postgres")
def test_auto_version_illegal_return_types():
    @materialize(input_type=pd.DataFrame, version=AUTO_VERSION)
    def blob():
        return Blob(1), Table(pd.DataFrame())

    @materialize(input_type=pd.DataFrame, version=AUTO_VERSION)
    def raw_sql():
        return RawSql("..."), Table(pd.DataFrame())

    with Flow() as f:
        with Stage("auto_version"):
            _blob = blob()
            _raw_sql = raw_sql()

    with swallowing_raises(ValueError, match="Blob"):
        f.run(_blob)

    with swallowing_raises(ValueError, match="RawSql"):
        f.run(_raw_sql)


def test_auto_version_not_supported():
    import sqlalchemy as sa

    @materialize(input_type=sa.Table, version=AUTO_VERSION)
    def not_supported():
        return Table(pd.DataFrame({"x": [1, 2, 3, 4]}))

    with Flow() as f:
        with Stage("auto_version"):
            _ = not_supported()

    with swallowing_raises(TypeError, match="Auto versioning not supported"):
        f.run()


# TODO: Currently we only test that auto versioning actually works,
#       and that the task gets called the expected amount of times
#       in the polars hook tests.
#       Once we have support for auto versioning with pandas, we
#       might also want to put some tests into this file.
