from pydiverse.pipedag.backend.table.util.sql_ddl import insert_into_in_query


def test_insert_into():
    test_pairs = {
        "Select 1": "Select 1 INTO a.b",
        "Select 1 WHERE TRUE": "Select 1 INTO a.b WHERE TRUE",
        "Select 1 GROUP\nBY x": "Select 1 INTO a.b GROUP\nBY x",
        "Select 1 FROM A GROUP BY x": "Select 1 INTO a.b FROM A GROUP BY x",
        "Select 1 UNION ALL SELECT 2": "Select 1 INTO a.b UNION ALL SELECT 2",
        "Select 1 From X": "Select 1 INTO a.b From X",
        "Select (SELECT 1 FROM Y) From X": "Select (SELECT 1 FROM Y) INTO a.b From X",
        "Select (SELECT (SELECT 1 FROM Z) FROM Y) From X": (
            "Select (SELECT (SELECT 1 FROM Z) FROM Y) INTO a.b From X"
        ),
    }
    for raw_query, expected_query in test_pairs.items():
        res = insert_into_in_query(raw_query, "a", "b")
        assert res == expected_query
