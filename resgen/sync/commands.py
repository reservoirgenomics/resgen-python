import getpass
import logging
import os
from pathlib import Path

import click
import resgen as rg
import os.path as op

from resgen.sync.folder import (
    get_local_datasets, get_remote_datasets, add_and_update_local_datasets, remove_stale_remote_datasets
)

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
@click.option("--name", default=None)
@click.option("--sync-full-path/--no-sync-full-path", default=False)
@click.option("-f", "--force-update", default=False)
def datasets(gruser, project, datasets, tag, sync_remote, name, sync_full_path,
             force_update):
    """Upload if a file with the same name doesn't already exist.

    If files are of the form "filename1,filename2" it will be assumed
    that filename2 is the index file for filename1.

    If -f/--force-update is specified, files will be uploaded even if they already exist
    in the project.
    """
    try:
        try:
            rgc = rg.connect()
        except rg.UnknownConnectionException:
            logger.error("Unable to login, please check your username and password")
            return

        project = rgc.find_or_create_project(project, group=gruser)

        metadata = {"tags": [{"name": t} for t in tag]}
        if name:
            metadata["name"] = name
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
                    sync_full_path=sync_full_path,
                    force_update=force_update,
                    **metadata,
                )
            else:
                logger.info(
                    "Syncing dataset: %s with metadata: %s",
                    parts[0],
                    str(metadata),
                )
                project.sync_dataset(
                    dataset,
                    sync_remote,
                    sync_full_path=sync_full_path,
                    force_update=force_update,
                    **metadata
                )
    except rg.InvalidCredentialsException:
        logger.error(
            "Invalid credentials. Make sure that they are set in either "
            "~/.resgen/credentials or in the environment variables RESGEN_USERNAME "
            "and RESGEN_PASSWORD."
        )

@click.command()
@click.argument("gruser")
@click.argument("project")
@click.argument('directory')
@click.option("-r", "--remove-old", default=False)
def folder(gruser, project, directory, remove_old):
    """Make sure all the datasets in the directory are represented in the
    resgen project.
    
    :param remove-old: Remove datasets which are no longer present in the
        directory. Defaults to false to prevent unwanted deletions.
    """
    try:
        try:
            rgc = rg.connect()
        except rg.UnknownConnectionException:
            logger.error("Unable to login, please check your username and password")
            return
        
        directory = op.abspath(directory)

        project = rgc.find_or_create_project(project, group=gruser)
        local_datasets = get_local_datasets(directory)
        remote_datasets = get_remote_datasets(project)

        add_and_update_local_datasets(project, local_datasets, remote_datasets, base_directory=directory, link=False)

        if remove_old:
            remove_stale_remote_datasets(project, local_datasets, remote_datasets, base_directory=directory, link=False)

    except rg.InvalidCredentialsException:
        logger.error(
            "Invalid credentials. Make sure that they are set in either "
            "~/.resgen/credentials or in the environment variables RESGEN_USERNAME "
            "and RESGEN_PASSWORD."
        )
    
sync.add_command(datasets)
sync.add_command(folder)
