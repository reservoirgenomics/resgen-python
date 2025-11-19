from os.path import join
from subprocess import run
import click
import os.path as op
import os
import logging
import resgen as rg
from resgen.sync.folder import (
    get_local_datasets,
    get_remote_datasets,
    add_and_update_local_datasets,
    remove_stale_remote_datasets,
)
from resgen.license import get_license, datasets_allowed, LicenseError
from resgen.exceptions import ResgenError
from resgen.utils import (
    tracktype_default_position,
    datatype_to_tracktype,
    infer_filetype,
    infer_datatype,
)

import sys

from slugid import nice

logger = logging.getLogger(__name__)

START_TEMPLATE = """
version: "3"
services:
  redis:
    image: "redis:alpine"

  rabbitmq:
    image: "rabbitmq"

  resgen:
    depends_on:
      - "redis"
    command: /var/task/start_service.sh
    image: "{image}"
    platform: "{platform}"
    ports:
      - {port}:80
    volumes:
      - {data_directory}:/data
      - {tmp_directory}:/tmp
      - {media_directory}:/media{aws_volume}
    environment:
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - RESGEN_API_HOST={api_host}
      - RESGEN_SECRET_KEY={resgen_secret_key}
      - RESGEN_LOCAL_VIEWS_DIR=/data/viewconfs/
      - RESGEN_CLIENT_SERVER=localhost
      - RESGEN_CLIENT_SCHEME=http
      - RESGEN_CLIENT_PORT=80
      - RESGEN_USER_SQLITE_DIR=/data/
      - RESGEN_S3_FILES_ENABLED=True
      - RESGEN_AWS_BUCKET=resgen-test
      - RESGEN_AWS_BUCKET_PREFIX=tmp
      - RESGEN_AWS_BUCKET_MOUNT_POINT=aws
      - RESGEN_MEDIA_ROOT=/media/
      - SITE_URL=localhost
      - CELERY_BROKER=rabbitmq:5672
      - RESGEN_LOCAL_JWT_AUTH=True
      - RESGEN_LOCAL_UPLOADS=True
      - SITE_URL=localhost:{port}
      - RESGEN_LICENSE_JWT={resgen_license_jwt}
    container_name: "resgen-server-container"
"""

LOGGED_SERVICES = ["nginx", "uwsgi", "celery"]


def get_license_text(base_directory: str) -> str:
    """Locate the license file in the resgen metadata
    in a base directory."""
    with open(join(base_directory, ".resgen/license.jwt"), "r") as f:
        return f.read()


def get_compose_file(base_directory):
    """Locate the docker compose file in the resgen metadata
    in a base directory."""
    return join(base_directory, ".resgen/config/stack.yml")


def get_secret_key(base_directory=None):
    """Locate the secret key in the resgen metadata
    in a base directory. If no base directory is specified then
    assume the secret key is at ~/.resgen/secret.key.

    If no secret key is present there, generate a new one and store
    it there.
    """
    if not base_directory:
        base_directory = os.expanduser("~/.resgen/")

    if not op.exists(base_directory):
        os.makedirs(base_directory, exist_ok=True)

    secret_key_file = join(base_directory, "secret.key")

    logger.info("Secret_key_file: %s", secret_key_file)

    if not op.exists(secret_key_file):
        # If there's no secret key file then return
        secret_key = nice()
    else:
        with open(secret_key_file, "r") as f:
            secret_key = f.read().strip()

            if not secret_key:
                # Empty secret key
                secret_key = nice()

    with open(secret_key_file, "w") as f:
        # Write the secret key back, just in case it wasn't previously generated
        f.write(secret_key)

    return secret_key


@click.group()
def manage():
    """Manage resgen deployments."""
    pass


DEFAULT_PORT = 1807
DEFAULT_IMAGE = "public.ecr.aws/s1s0v0c3/resgen:latest"


def _start(
    directory,
    license=None,
    port=1807,
    platform=None,
    image=DEFAULT_IMAGE,
    foreground=False,
    use_aws_creds=False,
):
    """Start a resgen instance in a directory.

    If there's an existing resgen DB in the directory, it will be used.

    If not, a new one will be created and populated with all of the files
    in the directory.
    """
    if not platform:
        from platform import version as platform_version

        if "ARM64" in platform_version():
            logger.info("Inferring arm64 platform from: %s", platform_version())
            platform = "linux/arm64/v8"
        else:
            platform = "linux/amd64"

    logger.info("Using platform: %s", platform)

    if not license:
        logger.warning(
            "No license file provided, default to guest license. "
            "This will limit the use of this software to a maximum of 20 files "
            "per project."
        )
        license_text = ""
    else:
        license_text = open(license, "r").read()

    compose_file = get_compose_file(directory)
    compose_directory = op.dirname(compose_file)

    if not op.exists(compose_directory):
        os.makedirs(compose_directory)

    data_directory = join(directory, ".resgen/data")
    tmp_directory = join(directory, ".resgen/tmp")
    media_directory = directory

    # Store the license in the resgen directory so that we don't
    # have to pass it in to the e.g. sync command
    with open(join(directory, ".resgen/license.jwt"), "w") as f:
        f.write(license_text)

    # Make sure the data and tmp directories exist
    # We may want to check that they have the right user permissions
    if not op.exists(data_directory):
        os.makedirs(data_directory, exist_ok=True)
    if not op.exists(tmp_directory):
        os.makedirs(tmp_directory, exist_ok=True)

    aws_volume = ""
    if use_aws_creds:
        aws_creds_path = os.path.expanduser("~/.aws")
        if os.path.exists(aws_creds_path):
            aws_volume = f"\n      - {aws_creds_path}:/root/.aws"

    with open(compose_file, "w") as f:
        compose_text = START_TEMPLATE.format(
            data_directory=data_directory,
            tmp_directory=tmp_directory,
            media_directory=media_directory,
            port=port,
            uid=os.getuid(),
            gid=os.getgid(),
            api_host=f"http://localhost:{port}/",
            resgen_license_jwt=license_text,
            platform=platform,
            resgen_secret_key=get_secret_key(base_directory=join(directory, ".resgen")),
            image=image,
            aws_volume=aws_volume,
        )

        f.write(compose_text)

    cmd = ["docker", "compose"]
    cmd += ["-f", compose_file, "up"]
    if not foreground:
        cmd += ["-d"]
    run(cmd)

    logger.info("Started local resgen on http://localhost:%d", port)


@manage.command()
@click.argument("directory")
@click.option("--license", type=str, help="The path to the license file to use")
@click.option("--port", type=int, default=DEFAULT_PORT, help="The port to execute on")
@click.option("--platform", default=None)
@click.option("--image", default=DEFAULT_IMAGE)
@click.option("--foreground", default=False, is_flag=True)
@click.option(
    "--use-aws-creds",
    default=False,
    is_flag=True,
    help="Mount ~/.aws credentials into container",
)
def start(directory, license, port, platform, image, foreground, use_aws_creds):
    """Start a resgen instance in a directory.

    If there's an existing resgen DB in the directory, it will be used.

    If not, a new one will be created and populated with all of the files
    in the directory.
    """
    _start(
        directory=directory,
        license=license,
        port=port,
        platform=platform,
        image=image,
        foreground=foreground,
        use_aws_creds=use_aws_creds,
    )


@manage.command()
@click.argument("directory")
def stop(directory):
    """Stop a running instance."""
    compose_file = get_compose_file(directory)

    run(["docker", "compose", "-f", compose_file, "down"])


@manage.command()
@click.argument("directory")
@click.argument("service", type=click.Choice(LOGGED_SERVICES), required=False)
def logs(directory, service=None):
    """Get the nginx logs for the resgen instance deployed at
    this directory."""
    tmp_dir = join(directory, ".resgen/tmp")

    if not service:
        services = LOGGED_SERVICES
    else:
        services = [service]

    for service in services:
        print(f"=== {service} ===")
        files = [f for f in os.listdir(tmp_dir) if f.startswith(f"{service}-stderr")]

        assert len(files) == 1, "There should be only one resgen error file"
        with open(join(tmp_dir, files[0]), "r") as f:
            print(f.read())


@manage.command()
@click.argument("directory")
@click.option("--style", type=click.Choice(["run", "exec"]), default="run")
def shell(directory, style):
    """Open a shell in the Docker container hosting the repo.

    The style specifies whether to open a shell in a new container or an
    existing one.
    """
    if style == "run":
        run(
            [
                "docker",
                "run",
                "-v",
                f"{directory}/.resgen/data/:/data",
                "-it",
                "resgen-server",
                "bash",
            ]
        )
    else:
        run(["docker", "exec", "-it", "resgen-server-container", "bash"])


def _create_user(directory, username, password):
    """Create a resgen user."""
    # print("-c", f"\"import django.contrib.auth; django.contrib.auth.models.User.objects.create_user('{username}', password='{password}')\"")
    compose_file = get_compose_file(directory)

    run(
        [
            "docker",
            "compose",
            "-f",
            compose_file,
            "run",
            "resgen",
            "python",
            "manage.py",
            "shell",
            "-c",
            f"import django.contrib.auth; django.contrib.auth.models.User.objects.create_user('{username}', password='{password}')",
        ]
    )


@manage.command()
@click.argument("directory")
def create_user(directory):
    """Add a user to a local resgen instance."""
    username = input("Username?\n")
    password = input("Password?\n")

    _create_user(directory, username, password)


@manage.command()
@click.argument("directory")
def create_superuser(directory):
    """Add a user to a local resgen instance."""
    compose_file = get_compose_file(directory)

    run(
        [
            "docker",
            "compose",
            "-f",
            compose_file,
            "run",
            "resgen",
            "python",
            "manage.py",
            "createsuperuser",
        ]
    )


def can_sync_datasets(directory, total_tilesets):
    license = get_license(join(directory, ".resgen/license.jwt"))

    # Only guest accounts are limited in how many datasets they
    # can add
    if license.permissions == "guest":

        if total_tilesets > datasets_allowed(license):
            raise LicenseError(
                f"Guest license has exceeded the number of datasets allowed ({datasets_allowed(license)})"
            )


def _sync_datasets(directory):
    """Make sure all the datasets in the directory are represented in the
    resgen project. The resgen project will be named after the directory's
    basename."""
    directory = op.abspath(directory)
    project_name = op.basename(directory)

    user = "local"
    password = "local"
    host = "http://localhost:1807"

    # TODO: Load the docker-compose file from the .resgen folder and
    # pull the port from there
    try:
        rgc = rg.connect(
            username=user, password=password, host=host, auth_provider="local"
        )
    except rg.UnknownConnectionException:
        logger.error("Unable to login, please check your username and password")
        return

    project = rgc.find_or_create_project(project_name)
    local_datasets = get_local_datasets(directory)
    remote_datasets = get_remote_datasets(project)

    can_sync_datasets(directory, len(local_datasets))

    try:
        add_and_update_local_datasets(
            project,
            local_datasets,
            remote_datasets,
            base_directory=directory,
            link=True,
        )
        remove_stale_remote_datasets(project, local_datasets, remote_datasets)
    except ResgenError as re:
        logger.error(str(re))


@manage.command()
@click.argument("directory")
def sync_datasets(directory):
    _sync_datasets(directory)


@manage.command()
def list():
    """List running resgen docker containers with their directories and ports."""
    import subprocess
    import json

    try:
        # Get running containers with resgen-server-container name pattern
        result = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                "name=resgen-server-container",
                "--format",
                "json",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        if not result.stdout.strip():
            print("No running resgen containers found.")
            return

        containers = []
        for line in result.stdout.strip().split("\n"):
            if line:
                containers.append(json.loads(line))

        rows = []

        for container in containers:
            container_id = container["ID"]

            # Get detailed container info including mounts and ports
            inspect_result = subprocess.run(
                ["docker", "inspect", container_id],
                capture_output=True,
                text=True,
                check=True,
            )

            inspect_data = json.loads(inspect_result.stdout)[0]

            # Extract port mapping
            ports = inspect_data.get("NetworkSettings", {}).get("Ports", {})
            port_mapping = "N/A"
            for container_port, host_bindings in ports.items():
                if host_bindings and container_port == "80/tcp":
                    port_mapping = host_bindings[0]["HostPort"]
                    break

            # Extract data directory mount
            mounts = inspect_data.get("Mounts", [])
            data_directory = "N/A"
            for mount in mounts:
                if mount.get("Destination") == "/data":
                    # Extract the parent directory (remove /.resgen/data)
                    source = mount.get("Source", "")
                    if source.endswith("/.resgen/data"):
                        data_directory = source[:-13]  # Remove '/.resgen/data'
                    else:
                        data_directory = source
                    break

            url = (
                f"https://localhost:{port_mapping}" if port_mapping != "N/A" else "N/A"
            )
            rows.append([data_directory, url, container["Status"]])

        # Print table header
        print(f"{'Directory':<50} {'URL':<25} {'Status':<20}")
        print("-" * 95)

        # Print table rows
        for row in rows:
            print(f"{row[0]:<50} {row[1]:<25} {row[2]:<20}")

    except subprocess.CalledProcessError as e:
        print(f"Error running docker command: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing docker output: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


@manage.command()
@click.argument("file")
@click.option("-ft", "--filetype", default=None)
@click.option("-dt", "--datatype", default=None)
@click.option("-tt", "--tracktype", default=None)
@click.option("-tp", "--track-position", default=None)
@click.option("-th", "--track-height", default=100)
@click.option("-t", "--tag", multiple=True, help="Pass in tags")
@click.option("--image", default=DEFAULT_IMAGE)
@click.option("--platform", default=None)
def view(
    file,
    filetype,
    datatype,
    tracktype,
    track_position,
    track_height,
    tag,
    image,
    platform,
):
    """View a dataset."""
    import webbrowser
    import time
    import requests

    logger.info("file_path %s", file)
    is_s3_file = file.startswith("s3://")
    if is_s3_file:
        directory = op.expanduser("~")
        file_path = file
    else:
        file_path = op.abspath(file)
        directory = op.dirname(file_path)
    logger.info(f"Directory {directory}")

    # Check if server is running at default location
    server_running = False
    try:
        response = requests.get(
            f"http://localhost:{DEFAULT_PORT}/api/v1/current/", timeout=2
        )
        server_running = response.status_code == 200
    except:
        server_running = False

    # Check if AWS credentials are needed and available for S3 files
    if is_s3_file and server_running:
        import subprocess

        try:
            # Check if AWS credentials are mounted in the running container
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "resgen-server-container",
                    "test",
                    "-f",
                    "/root/.aws/credentials",
                ],
                capture_output=True,
            )
            if result.returncode != 0:
                logger.info(
                    "AWS credentials not mounted, restarting container with credentials..."
                )
                stop(directory)
                server_running = False
        except subprocess.CalledProcessError:
            server_running = False

    # Start server if not running
    if not server_running:
        logger.info("No server running, starting resgen server...")
        _start(
            directory=directory,
            license=None,
            port=DEFAULT_PORT,
            image=image,
            foreground=False,
            platform=platform,
            use_aws_creds=is_s3_file,
        )

        # _create_user(directory, 'local', 'local')

        sys.stdout.write("Waiting for server to start...")
        sys.stdout.flush()
        # Wait for server to be ready
        for i in range(30):
            sys.stdout.write(".")
            sys.stdout.flush()

            try:
                response = requests.get(
                    f"http://localhost:{DEFAULT_PORT}/api/v1/tilesets/", timeout=2
                )
                if response.status_code == 200:
                    break
            except:
                pass
            time.sleep(1)

    sys.stdout.write("\n")
    sys.stdout.flush()
    # Sync datasets to ensure file is available
    # _sync_datasets(directory)

    # Create viewconf and open browser
    rgc = rg.connect(
        username="local",
        password="local",
        host=f"http://localhost:{DEFAULT_PORT}",
        auth_provider="local",
    )
    project = rgc.find_or_create_project(op.basename(directory))

    token = rgc.get_local_token()

    # Find the tileset for this file
    # TODO, we may want to use a hash or something to identify
    # this file. On the other hand, that would take a lot of tile
    filename = op.basename(file_path)
    tileset = None
    for ts in project.list_datasets():
        if filename in ts.datafile:
            tileset = ts
            break

    if not filetype:
        filetype = infer_filetype(file_path)
        logger.info(f"Inferred filetype: {filetype}")
    if not datatype:
        datatype = infer_datatype(filetype)
        logger.info(f"Inferred datatype: {datatype}")
    if not tracktype:
        tracktype, position = datatype_to_tracktype(datatype)
        logger.info(f"Inferred tracktype: {tracktype}")

    if not filetype:
        raise ValueError(f"Could not infer filetype for filename: {file_path}")

    if not tileset:
        # need to add the dataset
        if file_path.startswith("s3"):
            uuid = project.add_s3_dataset(file_path)
        else:
            dataset_rel_path = op.relpath(file_path, directory)
            logger.info("Adding link dataset", dataset_rel_path)
            uuid = project.add_link_dataset(dataset_rel_path)

        tileset = rgc.get_dataset(uuid)

    uuid = tileset.uuid
    tags = [
        {"name": f"filetype:{filetype}"},
        {"name": f"datatype:{datatype}"},
    ]

    for t in tag:
        tags += [{"name": t}]

    rgc.update_dataset(
        uuid,
        {"tags": tags},
    )

    # else:
    #     print("deleting")
    #     project.delete_dataset(tileset.uuid)

    from higlass import view

    track = tileset.hg_track(
        track_type=tracktype, position=track_position, height=track_height
    )
    viewconf = view(track)

    # Save viewconf and get URL
    saved_viewconf = project.add_viewconf(viewconf.viewconf().dict(), "View dataset")
    url = f"http://localhost:{DEFAULT_PORT}/viewer/{saved_viewconf['uuid']}?at={token['access_token']}&rt={token['refresh_token']}"

    logger.info(f"Opening {url}")
    webbrowser.open(url)
