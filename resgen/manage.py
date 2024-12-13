from os.path import join
from tempfile import TemporaryDirectory
from subprocess import run
import click
import os.path as op
import os
from typing import Literal
import logging
import resgen as rg

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
    environment:
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - RESGEN_API_HOST=/
      - RESGEN_USER_SQLITE_DIR=/data/
      - RESGEN_AWS_BUCKET=resgen-test
      - RESGEN_AWS_BUCKET_PREFIX=tmp
      - RESGEN_AWS_BUCKET_MOUNT_POINT=aws
      - SITE_URL=localhost
      - CELERY_BROKER=rabbitmq:5672
      - RESGEN_LOCAL_JWT_AUTH=True
      - SITE_URL=localhost:{port}
    container_name: "resgen-server-container"
"""

LOGGED_SERVICES = ['nginx', 'uwsgi', 'celery']

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
@click.option('--port', type=int, default=1807)
def start(directory, port):
    """Start a resgen instance in a directory.
    
    If there's an existing resgen DB in the directory, it will be used.

    If not, a new one will be created and populated with all of the files
    in the directory.

    :param directory: The directory to start and serve from
    :param port: The port to execute on
    """
    compose_file = get_compose_file(directory)
    compose_directory = op.dirname(compose_file)

    if not op.exists(compose_directory):
        os.makedirs(compose_directory)

    data_directory = join(directory, '.resgen/data')
    tmp_directory = join(directory, '.resgen/tmp')

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
            port=port,
            uid=os.getuid(),
            gid=os.getgid()
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

def get_local_datasets(directory):
    """Get a list of all local datasets within the directory."""
    local_datasets = []

    for path, folders, files in os.walk(directory):
        if path.startswith(join(directory, '.resgen')):
            continue

        for folder in folders:
            full_folder = join(path, folder)
            if full_folder.startswith(join(directory, '.resgen')):
                # skip the .resgen metadata directory
                continue

            local_datasets += [{
                "fullpath": op.relpath(full_folder, directory),
                "name": folder,
                "is_folder": True,
            }]
        for file in files:
            full_file = join(path, file)

            local_datasets += [{
                "fullpath": op.relpath(full_file, directory),
                "name": file,
                "is_folder": False,
            }]
    
    return local_datasets

def get_remote_datasets(project):
    """Get all remote datasets for the project.
    
    This function will consolidate paths based on containing folder ids.
    """
    datasets = project.list_datasets(limit=10000)
    ds_by_uid = dict([(ds.uuid, ds) for ds in datasets])
    ds_by_fullpath = {}

    remote_datasets = []

    for ds in datasets:
        filename = ds.name
        ds.fullname = ds.name

        ds_json = {
            "uuid": ds.uuid,
            "name": ds.name,
            "fullname": ds.name
        }

        while ds.containing_folder:
            ds1 = ds_by_uid[ds.containing_folder]
            filename = join(ds1.name, filename)

            ds = ds1

        ds_json['fullname'] = filename
        remote_datasets += [ds_json]

    return dict([(ds['fullname'], ds) for ds in remote_datasets])

@manage.command()
@click.argument('directory')
def sync_datasets(directory):
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


    import json
    print(json.dumps(local_datasets, indent=2))
    print(json.dumps(remote_datasets, indent=2))

    def get_parent_uuid(dataset):
        parent_dir = op.split(dataset['fullpath'])[0]
        parent = remote_datasets.get(
            parent_dir
        )
        if parent:
            parent = parent['uuid']
        
        return parent
    
    for dataset in local_datasets:
        if dataset['fullpath'] not in remote_datasets:
            if dataset['is_folder']:
                print("need to add", dataset['fullpath'])


                parent = get_parent_uuid(dataset)
                uuid = project.add_folder_dataset(
                    folder_name=dataset['name'],
                    parent=parent
                )
                remote_datasets[dataset['fullpath']] = {
                    'uuid': uuid,
                    "is_folder": True
                }
            else:
                # Handle adding a file dataset

                parent = get_parent_uuid(dataset)
                logger.info("Adding dataset name: %s datafile: %s, parent: %s", dataset['name'], dataset['fullpath'], parent)

                uuid = project.add_local_dataset(
                    datafile=dataset['fullpath'],
                    name=dataset['name'],
                    parent=parent
                )

                logger.info("Added with uuid: %s", uuid)
                remote_datasets[dataset['fullpath']] = {
                    'uuid': uuid,
                    "is_folder": False
                }

                print(remote_datasets)
            # split the dataset up into path parts and add each one with
            # its corresponding containing_folder uuid
            # if dataset['is_folder']:
                # project.add_folder_dataset(dataset['name'])


# Single instance

# All paths relative to project (base execution path)
#
# Get list of datasets from server
#  Resolve directory structure using containing_folders
# Get list of datasets from local directory
#  Group files with indexes
#
# Do we need functionality to add one dataset as an index
# to another?
#
#

# The problem with hosting one instance is that we would
# have to have on media directory at the root folder and
# that may present some security issues

