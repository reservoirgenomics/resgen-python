import getpass
import logging
import os
from pathlib import Path

import click
import resgen as rg

logger = logging.getLogger(__name__)


@click.group()
def sync():
    pass


@click.command()
@click.argument("gruser")
@click.argument("project")
@click.argument("datasets", nargs=-1)
@click.option("-t", "--tag", multiple=True)
@click.option("--sync-remote/--no-sync-remote", default=False)
def datasets(gruser, project, datasets, tag, sync_remote):
    """Upload if a file with the same name doesn't already exist.

    If files are of the form "filename1,filename2" it will be assumed
    that filename2 is the index file for filename1.
    """
    try:
        try:
            rgc = rg.connect()
        except rg.UnknownConnectionException:
            logger.error("Unable to login, please check your username and password")
            return

        project = rgc.find_or_create_project(project, group=gruser)

        metadata = {"tags": [{"name": t} for t in tag]}
        # metadata = dict([t.split(":")[:2] for t in tag])

        for dataset in datasets:
            parts = dataset.split(",")

            if len(parts) > 1:
                logger.info(
                    "Syncing dataset: %s with indexfile: %s and metadata: %s",
                    parts[0],
                    parts[1],
                    str(metadata),
                )
                project.sync_dataset(
                    parts[0],
                    index_filepath=parts[1],
                    sync_remote=sync_remote,
                    **metadata
                )
            else:
                logger.info(
                    "Syncing dataset: %s with metadata: %s", parts[0], str(metadata),
                )
                project.sync_dataset(dataset, sync_remote, **metadata)
    except rg.InvalidCredentialsException:
        logger.error(
            "Invalid credentials. Make sure that they are set in either "
            "~/.resgen/credentials or in the environment variables RESGEN_USERNAME "
            "and RESGEN_PASSWORD."
        )


sync.add_command(datasets)
