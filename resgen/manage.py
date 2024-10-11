from os.path import join
from tempfile import TemporaryDirectory
from subprocess import run
import click
import os.path as op
import os
from typing import Literal

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

      - RESGEN_USER_SQLITE_DIR=/data/
      - RESGEN_AWS_BUCKET=resgen-test
      - RESGEN_AWS_BUCKET_PREFIX=tmp
      - RESGEN_AWS_BUCKET_MOUNT_POINT=aws
      - SITE_URL=localhost
      - CELERY_BROKER=rabbitmq:5672
    container_name: "resgen-server-container"
"""

LOGGED_SERVICES = ['nginx', 'uwsgi', 'celery']

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
    compose_directory = join(directory, '.resgen/config')

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

    compose_file = join(compose_directory, 'stack.yml')
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
    """Open a shell in the Docker container hosting the repo."""
    print("style", style)
    if style=="run":
        run(["docker", "run", "-v", f"{directory}/.resgen/data/:/data", "-it",  "resgen-server", "bash"])
    else:
        run(["docker", "exec", "-it", "resgen-server-container", "bash"])
