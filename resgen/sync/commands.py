import click
import logging
import resgen as rg

from dotenv import load_dotenv
from pathlib import Path

logger = logging.getLogger(__name__)


@click.group()
def sync():
    pass


@click.command()
@click.argument("gruser")
@click.argument("project")
@click.argument("datasets", nargs=-1)
@click.option("-t", "--tag", multiple=True)
def datasets(gruser, project, datasets, tag):
    """Upload if a file with the same name doesn't already exist.

    If files are of the form "filename1,filename2" it will be assumed
    that filename2 is the index file for filename1.
    """
    try:
        env_path = Path.home() / ".resgen" / "credentials"

        load_dotenv(env_path)

        rgc = rg.connect()
        project = rgc.find_or_create_project(project, group=gruser)

        metadata = dict([t.split(":")[:2] for t in tag])

        for dataset in datasets:
            parts = dataset.split(",")

            if len(parts) > 1:
                logger.info(
                    "Syncing dataset: %s with indexfile: %s and metadata: %s",
                    parts[0],
                    parts[1],
                    str(metadata),
                )
                project.sync_dataset(parts[0], index_filepath=parts[1], **metadata)
            else:
                logger.info(
                    "Syncing dataset: %s with metadata: %s", parts[0], str(metadata),
                )
                project.sync_dataset(dataset, **metadata)
    except rg.InvalidCredentialsException:
        logger.error(
            "Invalid credentials. Make sure that they are set in either "
            "~/.resgen/credentials or in the environment variables RESGEN_USERNAME "
            "and RESGEN_PASSWORD."
        )


sync.add_command(datasets)
