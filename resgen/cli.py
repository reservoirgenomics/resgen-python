import click

from .list import commands as list_commands
from .sync import commands as sync_commands


@click.group()
def cli():
    pass


cli.add_command(sync_commands.sync)
cli.add_command(list_commands.list)
