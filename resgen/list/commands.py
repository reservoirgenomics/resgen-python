import getpass
import logging
import os
from pathlib import Path

import click
import resgen as rg

logger = logging.getLogger(__name__)


@click.group()
def list():
    pass


@click.command()
@click.argument("gruser")
def projects(gruser):
    """List the projects belonging to a group or user."""
    try:
        try:
            rgc = rg.connect()
        except rg.UnknownConnectionException:
            logger.error("Unable to login, please check your username and password")
            return

        projects = rgc.list_projects(gruser)

        for project in projects:
            print(project.name)
    except rg.InvalidCredentialsException:
        logger.error(
            "Invalid credentials. Make sure that they are set in either "
            "~/.resgen/credentials or in the environment variables RESGEN_USERNAME "
            "and RESGEN_PASSWORD."
        )


list.add_command(projects)
