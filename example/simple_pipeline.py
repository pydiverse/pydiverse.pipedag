from __future__ import annotations

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, materialize


# Define the different tasks our flow consists of
@materialize(version="1.0", nout=2)
def input_tables():
    names = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    ages = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "age": [20, 40, 60],
        }
    )

    return names, ages


@materialize(lazy=True, input_type=sa.Table)
def join_tables(names, ages):
    return sa.select(names.c.id, names.c.name, ages.c.age).join_from(
        names, ages, names.c.id == ages.c.id
    )


@materialize(input_type=pd.DataFrame)
def print_dataframe(df):
    print(df)


def main():
    # Define how the different tasks should be wired
    with Flow("flow") as flow:
        with Stage("inputs"):
            names, ages = input_tables()

        with Stage("features"):
            joined_table = join_tables(names, ages)
            print_dataframe(joined_table)

    # # In case you provide a pipedag.yaml, you can run the flow as simple as:
    # flow.run()

    # run flow with a duckdb configuration in a random temporary directory (this is
    # easier to get started)
    import tempfile

    from pydiverse.pipedag.core.config import create_basic_pipedag_config

    with tempfile.TemporaryDirectory() as temp_dir:
        cfg = create_basic_pipedag_config(
            f"duckdb:///{temp_dir}/db.duckdb",
            disable_stage_locking=True,  # This is special for duckdb
        ).get("default")
        # Execute the flow
        flow.run(config=cfg)


if __name__ == "__main__":
    from pydiverse.pipedag.util.structlog import setup_logging

    setup_logging()  # you can setup the logging and/or structlog libraries as you wish
    main()
