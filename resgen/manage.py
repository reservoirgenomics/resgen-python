from os.path import join
from tempfile import TemporaryDirectory
from subprocess import run
import click
import os.path as op
import os
from typing import Literal
import logging
import resgen as rg
from resgen.sync.folder import get_local_datasets, get_remote_datasets, add_and_update_local_datasets, remove_stale_remote_datasets
from resgen.license import get_license, datasets_allowed, LicenseError

logger = logging.getLogger(__name__)

START_TEMPLATE = """
version: "3"
services:
  redis:
    image: "redis:alpine"

  resgen:
    depends_on:
      - "redis"
    command: /var/task/start_service.sh
    image: "resgen-server"
    platform: "linux/amd64"
    ports:
      - {port}:80
    volumes:
      - {data_directory}:/data
      - {tmp_directory}:/tmp
      - {media_directory}:/media
    environment:
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - RESGEN_API_HOST={api_host}
      - RESGEN_LOCAL_VIEWS_DIR=/data/viewconfs/
      - RESGEN_USER_SQLITE_DIR=/data/
      - RESGEN_AWS_BUCKET=resgen-test
      - RESGEN_AWS_BUCKET_PREFIX=tmp
      - RESGEN_AWS_BUCKET_MOUNT_POINT=aws
      - RESGEN_MEDIA_ROOT=/media/
      - SITE_URL=localhost
      - CELERY_BROKER=rabbitmq:5672
      - RESGEN_LOCAL_JWT_AUTH=True
      - SITE_URL=localhost:{port}
      - RESGEN_LICENSE_JWT={resgen_license_jwt}
    container_name: "resgen-server-container"
"""

LOGGED_SERVICES = ['nginx', 'uwsgi', 'celery']

def get_license_text(base_directory: str) -> str:
    """Locate the license file in the resgen metadata
    in a base directory."""
    with open(join(base_directory, '.resgen/license.jwt'), 'r') as f:
        return f.read()

def get_compose_file(base_directory):
    """Locate the docker compose file in the resgen metadata
    in a base directory."""
    return join(base_directory, '.resgen/config/stack.yml')

@click.group()
def manage():
    """Manage resgen deployments."""
    pass

@manage.command()
@click.argument('directory')
@click.option('--license', type=str, help="The path to the license file to use")
@click.option('--port', type=int, default=1807, help="The port to execute on")
def start(directory, license, port):
    """Start a resgen instance in a directory.
    
    If there's an existing resgen DB in the directory, it will be used.

    If not, a new one will be created and populated with all of the files
    in the directory.
    """
    if not license:
        logger.warning("No license file provided, default to guest license. "
                    "This will limit the use of this software to a maximum of 20 files "
                    "per project.")
        license_text=""
    else:
        license_text = open(license, 'r').read()

    compose_file = get_compose_file(directory)
    compose_directory = op.dirname(compose_file)

    if not op.exists(compose_directory):
        os.makedirs(compose_directory)

    data_directory = join(directory, '.resgen/data')
    tmp_directory = join(directory, '.resgen/tmp')
    media_directory = directory

    # Store the license in the resgen directory so that we don't
    # have to pass it in to the e.g. sync command
    with open(join(directory, '.resgen/license.jwt'), 'w') as f:
        f.write(license_text)

    # Make sure the data and tmp directories exist
    # We may want to check that they have the right user permissions
    if not op.exists(data_directory):
        os.makedirs(data_directory, exist_ok=True)
    if not op.exists(tmp_directory):
        os.makedirs(tmp_directory, exist_ok=True)

    with open(compose_file, 'w') as f:
        compose_text = START_TEMPLATE.format(
            data_directory=data_directory,
            tmp_directory=tmp_directory,
            media_directory=media_directory,
            port=port,
            uid=os.getuid(),
            gid=os.getgid(),
            api_host=f"http://localhost:{port}/",
            resgen_license_jwt=license_text
        )

        f.write(compose_text)
    
    run(["docker", "compose", "-f", compose_file, "up"])


@manage.command()
@click.argument('directory')
def stop(directory):
    """Stop a running instance."""
    compose_file = get_compose_file(directory)

    run(["docker", "compose", "-f", compose_file, "down"])

@manage.command()
@click.argument('directory')
@click.argument('service', type=click.Choice(LOGGED_SERVICES), required=False)
def logs(directory, service = None):
    """Get the nginx logs for the resgen instance deployed at
    this directory."""
    tmp_dir = join(directory, '.resgen/tmp')

    if not service:
        services = LOGGED_SERVICES
    else:
        services = [service]

    for service in services:
        print(f"=== {service} ===")
        files = [f for f in os.listdir(tmp_dir) if f.startswith(f'{service}-stderr')]

        assert len(files) == 1, "There should be only one resgen error file"
        with open(join(tmp_dir, files[0]), 'r') as f:
            print(f.read())


@manage.command()
@click.argument('directory')
@click.option("--style", type=click.Choice(["run", "exec"]), default='run')
def shell(directory, style):
    """Open a shell in the Docker container hosting the repo.
    
    The style specifies whether to open a shell in a new container or an
    existing one.
    """
    if style=="run":
        run(["docker", "run", "-v", f"{directory}/.resgen/data/:/data", "-it",  "resgen-server", "bash"])
    else:
        run(["docker", "exec", "-it", "resgen-server-container", "bash"])


@manage.command()
@click.argument('directory')
def create_user(directory):
    """Add a user to a local resgen instance."""
    username = input("Username?\n")
    password = input("Password?\n")

    print("username", username, "password", password)
    print("-c", f"\"import django.contrib.auth; django.contrib.auth.models.User.objects.create_user('{username}', password='{password}')\"")
    compose_file  = get_compose_file(directory)

    run(["docker", "compose", "-f", compose_file,
         "run", "resgen", "python", "manage.py", "shell", "-c",
         f"import django.contrib.auth; django.contrib.auth.models.User.objects.create_user('{username}', password='{password}')"])

@manage.command()
@click.argument('directory')
def create_superuser(directory):
    """Add a user to a local resgen instance."""
    compose_file  = get_compose_file(directory)

    run(["docker", "compose", "-f", compose_file,
         "run", "resgen", "python", "manage.py", "createsuperuser"])

def can_sync_datasets(directory, total_tilesets):
    license = get_license(join(directory, '.resgen/license.jwt'))

    # Only guest accounts are limited in how many datasets they
    # can add
    if license.permissions == 'guest':

        if total_tilesets > datasets_allowed(license):
            raise LicenseError(f"Guest account has exceeded the number of datasets allowed ({datasets_allowed(license)})")


def _sync_datasets(directory):
    """Make sure all the datasets in the directory are represented in the
    resgen project. The resgen project will be named after the directory's
    basename."""
    directory = op.abspath(directory)
    project_name = op.basename(directory)

    user = 'local'
    password = 'local'
    host = 'http://localhost:1807'

    # TODO: Load the docker-compose file from the .resgen folder and
    # pull the port from there
    try:
        rgc = rg.connect(username=user, password=password, host=host, auth_provider="local")
    except rg.UnknownConnectionException:
        logger.error("Unable to login, please check your username and password")
        return
    
    project = rgc.find_or_create_project(project_name)
    local_datasets = get_local_datasets(directory)
    remote_datasets = get_remote_datasets(project)

    can_sync_datasets(directory, len(local_datasets))

    add_and_update_local_datasets(project, local_datasets, remote_datasets, base_directory=directory, link=True)
    remove_stale_remote_datasets(project, local_datasets, remote_datasets)

@manage.command()
@click.argument('directory')
def sync_datasets(directory):
    _sync_datasets(directory)
