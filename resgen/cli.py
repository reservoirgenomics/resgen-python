import click

from .sync import commands as sync_commands

@click.group()
def cli():
    pass

cli.add_command(sync_commands.sync)