from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path

import click


@click.group()
def cli():
    pass


def find_commands():
    commands_dir = Path(__file__).parent / "commands"
    return [
        name
        for _, name, ispkg in pkgutil.iter_modules([str(commands_dir)])
        if not ispkg and not name.startswith("_")
    ]


def load_command(command: str):
    importlib.import_module(f"pydiverse.pipedag.management.commands.{command}")


def dynamically_load_commands():
    for command in find_commands():
        load_command(command)


dynamically_load_commands()
