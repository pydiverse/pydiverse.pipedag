# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

from types import SimpleNamespace

import pytest
import sqlalchemy as sa

from pydiverse.pipedag.backend.table.sql.dialects.snowflake import SnowflakeTableStore
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore
from pydiverse.pipedag.context import StageCommitTechnique
from pydiverse.pipedag.core.stage import Stage


def make_store():
    store = SnowflakeTableStore.__new__(SnowflakeTableStore, "")
    store.schema_prefix = "p_"
    store.schema_suffix = "_s"
    store.hook_cache = {}
    return store


def assert_cached_payload(payload, expected_table_name):
    assert list(payload) == [expected_table_name]
    assert payload[expected_table_name][0]["name"] == "col"
    assert payload[expected_table_name][0]["nullable"] is False


def test_init_stage_clears_only_transaction_schema_cache(monkeypatch):
    store = make_store()
    stage = Stage("demo")

    def fake_init_stage(self, stage):
        stage.set_transaction_name("demo__odd")

    monkeypatch.setattr(SQLTableStore, "init_stage", fake_init_stage)

    final_schema = store.get_schema(stage.name).get()
    transaction_schema = store.get_schema("demo__odd").get()
    store.hook_cache[store._reflection_cache_key(final_schema)] = {"final": []}
    store.hook_cache[store._reflection_cache_key(transaction_schema)] = {"tx": []}

    store.init_stage(stage)

    assert store.hook_cache[store._reflection_cache_key(final_schema)] == {"final": []}
    assert store._reflection_cache_key(transaction_schema) not in store.hook_cache


@pytest.mark.parametrize(
    ("technique", "transaction_name", "keep_source"),
    [
        (StageCommitTechnique.SCHEMA_SWAP, "demo__tmp", False),
        (StageCommitTechnique.READ_VIEWS, "demo__odd", True),
    ],
)
def test_commit_stage_replaces_final_cache(monkeypatch, technique, transaction_name, keep_source):
    store = make_store()
    stage = Stage("demo")
    stage.set_transaction_name(transaction_name)

    monkeypatch.setattr(SQLTableStore, "commit_stage", lambda self, stage: None)
    monkeypatch.setattr(
        "pydiverse.pipedag.backend.table.sql.dialects.snowflake.ConfigContext.get",
        lambda: SimpleNamespace(stage_commit_technique=technique),
    )

    final_schema = store.get_schema(stage.name).get()
    transaction_schema = store.get_schema(stage.transaction_name).get()
    transaction_payload = {"tx_table": [{"name": "col", "nullable": False, "type": sa.Integer()}]}
    store.hook_cache[store._reflection_cache_key(final_schema)] = {"old_final": []}
    store.hook_cache[store._reflection_cache_key(transaction_schema)] = transaction_payload

    store.commit_stage(stage)

    assert_cached_payload(store.hook_cache[store._reflection_cache_key(final_schema)], "tx_table")
    if keep_source:
        assert_cached_payload(store.hook_cache[store._reflection_cache_key(transaction_schema)], "tx_table")
    else:
        assert store._reflection_cache_key(transaction_schema) not in store.hook_cache


def test_reflect_table_reloads_schema_cache_on_miss():
    class TestSnowflakeTableStore(SnowflakeTableStore):
        def _load_schema_reflection(self, schema: str):
            self.loads.append(schema)
            if len(self.loads) == 1:
                return {
                    "first_table": [
                        {"name": "first_col", "nullable": False, "type": sa.Integer()},
                    ]
                }
            return {
                "first_table": [
                    {"name": "first_col", "nullable": False, "type": sa.Integer()},
                ],
                "second_table": [
                    {"name": "second_col", "nullable": True, "type": sa.String()},
                ],
            }

    store = TestSnowflakeTableStore.__new__(TestSnowflakeTableStore, "")
    store.schema_prefix = ""
    store.schema_suffix = ""
    store.hook_cache = {}
    store.loads = []

    table = store.reflect_table("second_table", "demo_schema")

    assert store.loads == ["demo_schema", "demo_schema"]
    assert table.schema == "demo_schema"
    assert [col.name for col in table.columns] == ["second_col"]


def test_reflect_table_preserves_database_schema():
    class TestSnowflakeTableStore(SnowflakeTableStore):
        def _load_schema_reflection(self, schema: str):
            assert schema == "other_db.demo_schema"
            return {
                "mytable": [
                    {"name": "mycol", "nullable": False, "type": sa.Integer()},
                ]
            }

    store = TestSnowflakeTableStore.__new__(TestSnowflakeTableStore, "")
    store.schema_prefix = ""
    store.schema_suffix = ""
    store.hook_cache = {}

    table = store.reflect_table("mytable", "other_db.demo_schema")

    assert table.schema == "other_db.demo_schema"
    assert [col.name for col in table.columns] == ["mycol"]
