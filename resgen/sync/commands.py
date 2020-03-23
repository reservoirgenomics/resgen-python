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
@click.argument('datasets', nargs=-1)
@click.option('-t', '--tag', multiple=True)
def datasets(gruser, project, datasets, tag):
    """Upload if a file with the same name doesn't already exist."""
    try:
        env_path = Path.home() / '.resgen' / 'credentials'

        load_dotenv(env_path)

        rgc = rg.connect()
        project = rgc.find_or_create_project(project, group=gruser)

        metadata = dict([t.split(':')[:2] for t in tag])

        for dataset in datasets:
            logger.info("Syncing dataset: %s", dataset)
            project.sync_dataset(dataset, **metadata)
    except rg.InvalidCredentialsException:
        logger.error('Invalid credentials. Make sure that they are set in either '
            '~/.resgen/credentials or in the environment variables RESGEN_USERNAME '
            'and RESGEN_PASSWORD.')


sync.add_command(datasets)