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
@click.argument('gruser')
@click.argument('project')
@click.argument('dataset')
def dataset(gruser, project, dataset):
    """Upload if a file with the same name doesn't already exist."""
    print("sync dataset")
    try:
        env_path = Path.home() / '.resgen' / 'credentials'

        load_dotenv(env_path)

        rgc = rg.connect()
        project = rgc.find_or_create_project(project, group=gruser)
        project.sync_dataset(dataset)
    except rg.InvalidCredentialsException:
        logger.error('Invalid credentials. Make sure that they are set in either '
            '~/.resgen/credentials or in the environment variables RESGEN_USERNAME '
            'and RESGEN_PASSWORD.')


sync.add_command(dataset)