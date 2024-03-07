from __future__ import annotations

import pandas as pd

from pydiverse.pipedag import Flow, Stage, StageLockContext
from tests.fixtures.instances import with_instances
from tests.util import tasks_library as m


@with_instances("postgres", "local_table_cache", "local_table_cache_inout")
def test_get_output_from_store():
    with Flow() as f:
        with Stage("stage_1"):
            df1 = m.pd_dataframe({"x": [0, 1, 2, 3]})
            df2 = m.pd_dataframe({"y": [0, 1, 2, 3]})
            dataframes = m.create_tuple(df1, df2)

    # We only use the StageLockContext for testing
    with StageLockContext():
        result = f.run()

        # Call on MaterializingTask
        pd.testing.assert_frame_equal(
            df1.get_output_from_store(as_type=pd.DataFrame),
            result.get(df1, as_type=pd.DataFrame),
        )

        pd.testing.assert_frame_equal(
            df2.get_output_from_store(as_type=pd.DataFrame),
            result.get(df2, as_type=pd.DataFrame),
        )

        pd.testing.assert_frame_equal(
            dataframes.get_output_from_store(as_type=pd.DataFrame)[1],
            result.get(dataframes, as_type=pd.DataFrame)[1],
        )

        # Call on MaterializingTaskGetItem
        pd.testing.assert_frame_equal(
            dataframes[0].get_output_from_store(as_type=pd.DataFrame),
            dataframes.get_output_from_store(as_type=pd.DataFrame)[0],
        )


@with_instances("postgres")
def test_call_task_outside_flow():
    # Literal Values
    assert m.one() == 1
    assert m.create_tuple(m.one(), m.two()) == (1, 2)
    assert m.noop("foo") == "foo"

    # Table
    df = m.pd_dataframe({"x": [0, 1, 2, 3]})
    expected = pd.DataFrame({"x": [0, 1, 2, 3]})

    pd.testing.assert_frame_equal(df, expected)
    m.assert_table_equal(df, expected)

    # Blob
    expected_blob = {"a": {"b": 1, "c": 2}, "d": [1, (2, 3)]}
    blob = m.as_blob(expected_blob)

    assert blob is expected_blob
    m.assert_blob_equal(blob, expected_blob)

    # RawSql
    expected_query = "SELECT 1 as x"
    raw_sql = m.as_raw_sql(expected_query)

    assert raw_sql == expected_query
