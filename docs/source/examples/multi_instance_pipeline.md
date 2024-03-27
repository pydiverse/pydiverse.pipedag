# Multiple instances of a data pipeline: full, mini, midi

In general, data pipelines are processing a considerable amount of information be it tables with 100k to 100 million rows
or even billions. Thus processing times will be many minutes or hours. However, iteration speed of software development
on the pipeline is key because the pipeline is used to transform the data in a way that increases understanding and from
better understanding come changes to the code in the data pipeline.

As a consequence, you should not just have one data pipeline. You should always have at least two little siblings for any
pipeline:
* mini: The minimal amount of data that allows the pipeline code to run through technically.
* midi: A somewhat reasonable selection of data which reaches a high level of code coverage, triggers most edge cases the
pipeline code is concerned with, and may be sampled in a way that allows for statistically sound conclusions be it with
reduced statistical prediction power or higher error margins. If all goals cannot be met with one subset of the input
data, more pipeline instances may be needed.

In pydiverse.pipedag, there is support for multiple pipeline instances readily built in. The idea is that there is a base
configuration (instance __any__) on which more specialized instance definitions may be based. Nearly every aspect can be
configured per pipeline instance. The most important setting to be different is the `instance_id`. The `instance_id` must
either be a part of the database engine URL (i.e. database name) or a schema prefix or suffix or a component in path
arguments specifying directories where a pipeline instance puts files. This way it can be guaranteed that two pipeline
instances never overwrite any data from each other. It is also possible to specify an arbitrary dictionary of values
in the `attrs` section which can be used at pipeline construction time and even fed into tasks for processing at runtime.

Here is an example pipeline configuration with multiple instances:

Please save the following file as pipedag.yaml or download the following [zip](multi_instance_pipeline.zip):
```yaml
name: data_pipeline
table_store_connections:
  postgres:
    args:
      url: "postgresql://{$POSTGRES_USERNAME}:{$POSTGRES_PASSWORD}@127.0.0.1:6543/{instance_id}"

  postgres2:
    args:
      # Postgres: this can be used after running `docker-compose up`
      url: "postgresql://{username}:{password}@{host}:{port}/{instance_id}"
      url_attrs_file: "{$POSTGRES_PASSWORD_CFG}"

blob_store_connections:
  file:
    args:
      base_path: "/tmp/pipedag/blobs"

technical_setups:
  default:
    # listen-interface for pipedag context server which synchronizes some task state during DAG execution
    network_interface: "127.0.0.1"
    # classes to be materialized to table store even without pipedag Table wrapper
    auto_table: ["pandas.DataFrame", "sqlalchemy.sql.expression.TextClause", "sqlalchemy.sql.expression.Selectable"]
    # abort as fast a possible on task failure and print most readable stack trace
    fail_fast: true

    # Attention: For disable_kroki: false, stage and task names might be sent to the kroki_url.
    #   You can self-host kroki if you like:
    #   https://docs.kroki.io/kroki/setup/install/
    disable_kroki: true
    kroki_url: "https://kroki.io"

    instance_id: pipedag_default

    cache_validation:
      mode: "normal"
      disable_cache_function: false
      ignore_task_version: false

    table_store:
      class: "pydiverse.pipedag.backend.table.SQLTableStore"

      # Postgres: this can be used after running `docker-compose up`
      table_store_connection: postgres2

      args:
        create_database_if_not_exists: True
        # print select statements before being encapsualted in materialize expressions and tables before writing to
        # database
        print_materialize: true
        # print final sql statements
        print_sql: true

    blob_store:
      class: "pydiverse.pipedag.backend.blob.FileBlobStore"
      blob_store_connection: file

    lock_manager:
      class: "pydiverse.pipedag.backend.lock.DatabaseLockManager"

    orchestration:
      class: "pydiverse.pipedag.engine.SequentialEngine"

instances:
  __any__:
    technical_setup: default
    # The following Attributes are handed over to the flow implementation (pipedag does not care)
    attrs:
      # by default we load source data and not a sampled version of a loaded database
      copy_filtered_input: false

  full:
    # Full dataset is using default database connection and schemas
    instance_id: pipedag_full
    table_store:
      table_store_connection: postgres2

  midi:
    # Full dataset is using default database connection and schemas
    instance_id: pipedag_midi
    attrs:
      # copy filtered input from full instance
      copy_filtered_input: true
      copy_source: full
      copy_per_user: false
      copy_filter_cnt: 2  # this is just dummy input where we sample 2 rows

  mini:
    # Full dataset is using default database connection and schemas
    instance_id: pipedag_mini
    attrs:
      copy_filtered_input: true
      copy_source: full
      copy_per_user: false
      copy_filter_cnt: 1  # this is just dummy input where we sample 1 row
```

This can be used with the following code:

```python
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.core.config import PipedagConfig
from pydiverse.pipedag.util.structlog import setup_logging


# these global variables are just a mock for information that can be sourced from somewhere for the full pipeline
dfA_source = pd.DataFrame(
    {
        "a": [0, 1, 2, 4],
        "b": [9, 8, 7, 6],
    }
)
dfA = dfA_source.copy()
input_hash = hash(str(dfA))


def has_new_input():
    global input_hash
    return input_hash


@materialize(nout=2, cache=has_new_input, version="1.0")
def input_task():
    global dfA
    return Table(dfA, "dfA"), Table(dfA, "dfB")


def has_copy_source_fresh_input(
    stage: Stage, attrs: dict[str, Any], pipedag_config: PipedagConfig
):
    source = attrs["copy_source"]
    per_user = attrs["copy_per_user"]
    source_cfg = pipedag_config.get(instance=source, per_user=per_user)
    with source_cfg:
        _hash = source_cfg.store.table_store.get_stage_hash(stage)
    return _hash


@materialize(input_type=pd.DataFrame, cache=has_copy_source_fresh_input, version="1.0")
def copy_filtered_inputs(
    stage: Stage, attrs: dict[str, Any], pipedag_config: PipedagConfig
):
    source = attrs["copy_source"]
    per_user = attrs["copy_per_user"]
    filter_cnt = attrs["copy_filter_cnt"]
    tbls = _get_source_tbls(source, per_user, stage, pipedag_config)
    ret = [Table(tbl.head(filter_cnt), name) for name, tbl in tbls.items()]
    return ret


def _get_source_tbls(source, per_user, stage, pipedag_config):
    source_cfg = pipedag_config.get(instance=source, per_user=per_user)
    with source_cfg:
        # This is just quick hack code to copy data from one pipeline instance to
        # another in a filtered way. It justifies actually a complete pydiverse
        # package called pydiverse.testdata. We want to achieve loose coupling by
        # pipedag transporting uninterpreted attrs with user code feeding the
        # attributes in testdata functionality
        engine = source_cfg.store.table_store.engine
        schema = source_cfg.store.table_store.get_schema(stage.name)
        meta = sa.MetaData()
        meta.reflect(bind=engine, schema=schema.name)
        tbls = {
            tbl.name: pd.read_sql_table(tbl.name, con=engine, schema=schema.name)
            for tbl in meta.tables.values()
        }
    return tbls


@materialize(input_type=pd.DataFrame, version="1.0")
def double_values(df: pd.DataFrame):
    return Table(df.transform(lambda x: x * 2))


@materialize(nout=2, input_type=sa.Table, lazy=True)
def extract_a_b(tbls: list[sa.Table]):
    a = [tbl for tbl in tbls if tbl.original.name == "dfa"][0]
    b = [tbl for tbl in tbls if tbl.original.name == "dfb"][0]
    return a, b


# noinspection PyTypeChecker
def get_flow(attrs: dict[str, Any], pipedag_config):
    with Flow("test_instance_selection") as flow:
        with Stage("stage_1") as stage:
            if not attrs["copy_filtered_input"]:
                a, b = input_task()
            else:
                tbls = copy_filtered_inputs(stage, attrs, pipedag_config)
                a, b = extract_a_b(tbls)
            a2 = double_values(a)

        with Stage("stage_2"):
            b2 = double_values(b)
            a3 = double_values(a2)

    return flow, b2, a3


def check_result(result, out1, out2, *, head=999):
    assert result.successful
    v_out1, v_out2 = result.get(out1), result.get(out2)
    pd.testing.assert_frame_equal(dfA_source.head(head) * 2, v_out1, check_dtype=False)
    pd.testing.assert_frame_equal(dfA_source.head(head) * 4, v_out2, check_dtype=False)


if __name__ == "__main__":
    password_cfg_path = str(Path(__file__).parent / "postgres_password.yaml")

    old_environ = dict(os.environ)
    os.environ["POSTGRES_PASSWORD_CFG"] = password_cfg_path

    setup_logging()  # you can setup the logging and/or structlog libraries as you wish
    pipedag_config = PipedagConfig(Path(__file__).parent / "pipedag.yaml")

    cfg = pipedag_config.get(instance="full")
    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    with StageLockContext():
        result = flow.run(config=cfg)
        check_result(result, out1, out2)
```

The file [multi_instance_pipeline.zip](multi_instance_pipeline.zip) includes a slighly more sophisticated project setup
even though it is still missing separate src and tests folders. Furthermore, it lacks proper configuration of pytest
markers. However, it already configures mini/midi pipelines to source data from the full pipeline.
See [best practices for configuring instances](best_practice_instances) for an even more realistic setup of pipeline
instances distinguishing fresh and stable pipelines.

`multi_instance_pipeline.zip` can be used as follows:

```bash
unzip multi_instance_pipeline.zip
cd multi_instance_pipeline
conda env create
conda activate multi_instance_pipeline
docker-compose up
```

and in another terminal within the same directory:

```bash
cd multi_instance_pipeline
conda activate multi_instance_pipeline
# for full pipeline
python multi_instance_pipeline.py
# for running mini/midi as unit tests
pytest -s -v
```