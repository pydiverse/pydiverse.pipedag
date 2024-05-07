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
  postgres_instances:
    args:
      url: "postgresql://{username}:{password}@{host}:{port}/pipedag"
      url_attrs_file: "{$POSTGRES_PASSWORD_CFG}"
      # using one database for multiple instances enables queries across instances
      schema_prefix: "{instance_id}_"

  postgres:
    args:
      # Postgres: this can be used after running `docker-compose up`
      url: "postgresql://{$POSTGRES_USERNAME}:{$POSTGRES_PASSWORD}@127.0.0.1:6543/pipedag"

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
      table_store_connection: postgres

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
    instance_id: full
    table_store:
      table_store_connection: postgres_instances

  midi:
    # Full dataset is using default database connection and schemas
    instance_id: midi
    table_store:
      table_store_connection: postgres_instances
    attrs:
      # copy filtered input from full instance
      copy_filtered_input: true
      copy_source: full
      copy_per_user: false
      copy_filter_cnt: 2  # this is just dummy input where we sample 2 rows

  mini:
    # Full dataset is using default database connection and schemas
    instance_id: mini
    table_store:
      table_store_connection: postgres_instances
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

from pydiverse.pipedag import (
    Flow,
    Stage,
    Table,
    materialize,
    input_stage_versions,
    ConfigContext,
)
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
    tbls: dict[str, sa.sql.expression.Alias],
    other_tbls: dict[str, sa.sql.expression.Alias],
    source_cfg: ConfigContext,
    *,
    attrs: dict[str, Any],
    stage: Stage,
):
    _ = tbls, other_tbls, attrs
    with source_cfg:
        _hash = source_cfg.store.table_store.get_stage_hash(stage)
    return _hash


@input_stage_versions(
    input_type=sa.Table,
    cache=has_copy_source_fresh_input,
    pass_args=["attrs", "stage"],
    lazy=True,
)
def copy_filtered_inputs(
    tbls: dict[str, sa.sql.expression.Alias],
    source_tbls: dict[str, sa.sql.expression.Alias],
    source_cfg: ConfigContext,
    *,
    attrs: dict[str, Any],
    stage: Stage,
):
    # we assue that tables can be copied within database engine just from different schema
    _ = source_cfg, stage
    # we expect this schema to be still empty, one could check for collisions
    _ = tbls
    filter_cnt = attrs["copy_filter_cnt"]
    ret = {
        name.lower(): Table(sa.select(tbl).limit(filter_cnt), name)
        for name, tbl in source_tbls.items()
    }
    return ret


@materialize(input_type=pd.DataFrame, version="1.0")
def double_values(df: pd.DataFrame):
    return Table(df.transform(lambda x: x * 2))


# noinspection PyTypeChecker
def get_flow(attrs: dict[str, Any], pipedag_config):
    with Flow("test_instance_selection") as flow:
        with Stage("stage_1") as stage:
            if not attrs["copy_filtered_input"]:
                a, b = input_task()
            else:
                other_cfg = pipedag_config.get(
                    attrs["copy_source"], attrs["copy_per_user"]
                )
                tbls = copy_filtered_inputs(other_cfg, stage=stage, attrs=attrs)
                a, b = tbls["dfa"], tbls["dfb"]
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
See [best practices for configuring instances](/examples/best_practices_instances) for an even more realistic setup of pipeline
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