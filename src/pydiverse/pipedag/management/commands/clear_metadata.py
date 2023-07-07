from __future__ import annotations

import click

from pydiverse.pipedag import PipedagConfig
from pydiverse.pipedag.backend.table import SQLTableStore
from pydiverse.pipedag.backend.table.sql.ddl import DropSchema
from pydiverse.pipedag.management.cli import cli


@cli.command()
@click.option(
    "--config",
    "config_path",
    type=str,
    help="path of the pipedag config file to use",
)
@click.option(
    "--instance",
    required=True,
    type=str,
    prompt=True,
    help="name of the instance to load from the config file",
)
@click.option(
    "--flow",
    type=str,
    help="name of the flow to load from the config file",
)
@click.option(
    "--per-user",
    is_flag=True,
    default=False,
)
@click.confirmation_option(
    prompt=(
        "Are you sure that you want to clear all metadata? "
        "This action can't be undone."
    )
)
def clear_metadata(
    config_path: str | None,
    instance: str,
    flow: str | None,
    per_user: bool,
):
    """Clears all pipedag metadata."""

    if config_path:
        pipedag_config = PipedagConfig(path=config_path)
    else:
        pipedag_config = PipedagConfig.default

    config = pipedag_config.get(
        instance=instance,
        flow=flow,
        per_user=per_user,
    )

    with config:
        table_store: SQLTableStore = config.store.table_store

        assert isinstance(
            table_store, SQLTableStore
        ), "clear-metadata only supported for SQLTableStore"

        drop_schema = DropSchema(
            table_store.metadata_schema,
            if_exists=True,
            cascade=True,
            engine=table_store.engine,
        )

        table_store.execute(drop_schema)

    click.echo("Did clear all metadata.")
