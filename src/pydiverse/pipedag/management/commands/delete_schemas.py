from __future__ import annotations

import click
import sqlalchemy as sa

from pydiverse.pipedag import PipedagConfig
from pydiverse.pipedag.backend.table import SQLTableStore
from pydiverse.pipedag.backend.table.sql.ddl import DropSchema, Schema
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
@click.option(
    "--yes",
    is_flag=True,
    help="Confirm the action without prompting.",
)
def delete_schemas(
    config_path: str | None,
    instance: str,
    flow: str | None,
    per_user: bool,
    yes: bool,
):
    """
    Delete all schemas associated with an instance.

    Only works with SQLTableStore.
    """

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
        ), "delete-schemas only supported for SQLTableStore"

        prefix = table_store.schema_prefix
        suffix = table_store.schema_suffix

        inspector = sa.inspect(table_store.engine)
        schema_names = inspector.get_schema_names()
        schema_names = [
            schema
            for schema in schema_names
            if schema.startswith(prefix) and schema.endswith(suffix)
        ]

        if len(schema_names) == 0:
            click.echo("No matching schemas found. Aborting.")
            exit()

        database = table_store.engine_url.database
        click.echo(f"Found the following schemas (in database '{database}'):")
        for schema in schema_names:
            click.echo(f"- {schema}")

        if not yes:
            click.confirm(
                "Are you sure you want to continue? "
                "This will delete all the schemas listed above. "
                "This action can't be undone.",
                abort=True,
            )

        schemas = [Schema(name, "", "") for name in schema_names]
        for schema in schemas:
            drop_schema = DropSchema(schema, cascade=True, engine=table_store.engine)
            table_store.execute(drop_schema)

    click.echo("Did delete all schemas.")
