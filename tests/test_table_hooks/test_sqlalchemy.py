from __future__ import annotations

import sqlalchemy as sa

from pydiverse.pipedag import *
from tests.util import tasks_library as m


def test_input_type_table():
    @materialize(input_type=sa.Table)
    def assert_is_table(t):
        assert isinstance(t, sa.Table)

    with Flow() as f:
        with Stage("stage"):
            x = m.simple_dataframe()
            assert_is_table(x)

    f.run()


def test_input_type_alias():
    @materialize(input_type=sa.Alias)
    def assert_is_alias(t):
        assert isinstance(t, sa.Alias)

    with Flow() as f:
        with Stage("stage"):
            x = m.simple_dataframe()
            assert_is_alias(x)

    f.run()
