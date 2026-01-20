from os.path import join
from subprocess import run
import click
import os.path as op
import os
import logging
import hashlib
import resgen as rg
from os.path import abspath
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
    container_name: "rgc-{name}"
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
    port=None,
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
    # Check if AWS credentials are loaded in existing container
    running_containers = _get_running_containers(image)
    current_container = next(
        (c for c in running_containers if c["directory"] == directory), None
    )

    if current_container:
        if use_aws_creds:
            # If we need to use aws credentials then we need to make sure
            # they're mounted in the docker container
            import subprocess

            try:
                result = subprocess.run(
                    [
                        "docker",
                        "exec",
                        current_container["id"],
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
                    current_container = None
            except subprocess.CalledProcessError:
                pass
        else:
            return current_container["port"]

    # Auto-assign port if not provided
    if port is None:
        if current_container:
            port = current_container["port"]
        else:
            used_ports = {c["port"] for c in running_containers}
            port = DEFAULT_PORT
            while port in used_ports:
                port += 1

    if not platform:
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

    print("data_directory", data_directory)

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
        directory_hash = hashlib.md5(abspath(directory).encode()).hexdigest()[:8]
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
            name=directory_hash,
        )

        f.write(compose_text)

    cmd = ["docker", "compose"]
    cmd += ["--project-directory", "."]
    cmd += ["-f", compose_file, "up"]
    if not foreground:
        cmd += ["-d"]
    run(cmd)

    logger.info("Started local resgen on http://localhost:%d", port)
    return port


@manage.command()
@click.argument("directory", default=".")
@click.option("--license", type=str, help="The path to the license file to use")
@click.option(
    "--port",
    type=int,
    default=None,
    help="The port to execute on (auto-assigned if not specified)",
)
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
@click.argument("directory", default=".")
def stop(directory):
    """Stop a running instance."""
    compose_file = get_compose_file(directory)

    run(["docker", "compose", "-f", compose_file, "down"])


@manage.command()
@click.argument("directory", default=".")
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
        if not files:
            print("No logs found", file=sys.stderr)
            continue

        assert len(files) == 1, "There should be only one resgen error file"
        with open(join(tmp_dir, files[0]), "r") as f:
            print(f.read())


@manage.command()
@click.argument("directory", default=".")
@click.option("--style", type=click.Choice(["run", "exec"]), default="run")
def shell(directory, style):
    """Open a shell in the Docker container hosting the repo.

    The style specifies whether to open a shell in a new container or an
    existing one.
    """
    directory_hash = hashlib.md5(abspath(directory).encode()).hexdigest()[:8]

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
        run(["docker", "exec", "-it", f"rgc-{directory_hash}", "bash"])


def _create_user(directory, username, password):
    """Create a resgen user."""
    # print("-c", f"\"import django.contrib.auth; django.contrib.auth.models.User.objects.create_user('{username}', password='{password}')\"")
    compose_file = get_compose_file(directory)
    print("compose_file", compose_file)

    run(
        [
            "docker",
            "compose",
            "--project-directory",
            directory,
            "-f",
            compose_file,
            "run",
            "resgen",
            "python",
            "manage.py",
            "shell",
            "-c",
            f"import django.contrib.auth; django.contrib.auth.models.User.objects.create_user('{username}', password='{password}')",
            # f"import os; print(os.environ)",
        ]
    )


@manage.command()
@click.argument("directory", default=".")
def create_user(directory):
    """Add a user to a local resgen instance."""
    username = input("Username?\n")
    password = input("Password?\n")

    _create_user(directory, username, password)


@manage.command()
@click.argument("directory", default=".")
def create_superuser(directory):
    """Add a user to a local resgen instance."""
    compose_file = get_compose_file(directory)

    run(
        [
            "docker",
            "compose",
            "--project-directory",
            directory,
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


def _get_directory_url(directory, image=DEFAULT_IMAGE):
    """Get the URL for a running resgen container in the specified directory."""
    containers = _get_running_containers(image=image)
    directory = op.abspath(directory)

    container = next((c for c in containers if c["directory"] == directory), None)

    if not container:
        return None

    return f"http://localhost:{container['port']}"


def _sync_datasets(directory, image=DEFAULT_IMAGE):
    """Make sure all the datasets in the directory are represented in the
    resgen project. The resgen project will be named after the directory's
    basename."""
    directory = op.abspath(directory)
    project_name = op.basename(directory)

    user = "local"
    password = "local"
    host = _get_directory_url(directory, image=image)

    if not host:
        logger.error(f"No running resgen container found for directory: {directory}")
        return

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
@click.argument("directory", default=".")
@click.option("-i", "--image", default=DEFAULT_IMAGE)
def sync_datasets(directory, image):
    _sync_datasets(directory, image=image)


def _get_running_containers(image=DEFAULT_IMAGE):
    """Get running resgen containers data."""
    import subprocess
    import json

    try:
        result = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                f"ancestor={image}",
                "--format",
                "json",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        if not result.stdout.strip():
            return []

        containers = []
        for line in result.stdout.strip().split("\n"):
            if line:
                container = json.loads(line)
                inspect_result = subprocess.run(
                    ["docker", "inspect", container["ID"]],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                inspect_data = json.loads(inspect_result.stdout)[0]

                # Extract port
                ports = inspect_data.get("NetworkSettings", {}).get("Ports", {})
                port = None
                for container_port, host_bindings in ports.items():
                    if host_bindings and container_port == "80/tcp":
                        port = int(host_bindings[0]["HostPort"])
                        break

                # Extract directory
                directory = None
                for mount in inspect_data.get("Mounts", []):
                    if mount.get("Destination") == "/data":
                        source = mount.get("Source", "")
                        directory = (
                            source[:-13] if source.endswith("/.resgen/data") else source
                        )
                        break

                if port and directory:
                    containers.append(
                        {"directory": directory, "port": port, "id": container["ID"]}
                    )

        return containers
    except Exception as e:
        logger.error(f"Error getting running containers: {e}")
        return []


def _list_containers(image=DEFAULT_IMAGE):
    """Print running resgen containers."""
    containers = _get_running_containers(image)

    if not containers:
        print("No running resgen containers found.")
        return

    print(f"{'URL':<25} {'Status':<20} {'Directory':<50}")
    print("-" * 95)

    for container in containers:
        url = f"http://localhost:{container['port']}"
        print(f"{url:<25} {'Running':<20} {container['directory']:<50}\n")


@manage.command()
@click.option("-i", "--image", default=DEFAULT_IMAGE)
def list(image):
    """List running resgen docker containers with their directories and ports."""
    _list_containers(image)


@manage.command()
@click.option("-i", "--image", default=DEFAULT_IMAGE)
def ls(image):
    """List running resgen docker containers with their directories and ports."""
    _list_containers(image)


@manage.command("open")
@click.argument("directory", default=".")
def cli_open(directory):
    """Open a browser to the server running in the specified directory."""
    import webbrowser

    url = _get_directory_url(directory)

    if not url:
        logger.error(f"No running resgen container found for directory: {directory}")
        return

    logger.info(f"Opening {url}")
    webbrowser.open(url)


@manage.command()
@click.option("-i", "--image", default=DEFAULT_IMAGE)
def update(image):
    """Pull the latest resgen image.

    Does the same thing as the "pull" command below
    """
    run(["docker", "pull", image])
    logger.info(f"Updated image: {image}")


@manage.command()
@click.option("-i", "--image", default=DEFAULT_IMAGE)
def pull(image):
    """Pull the latest resgen image.

    Alias of "update"
    """
    run(["docker", "pull", image])
    logger.info(f"Updated image: {image}")


def fill_in_filetype_datatype_tracktype(file_path, filetype, datatype, tracktype):
    """Fill in missing filetype, datatype and tracktype values by inferring them from a file.

    This function takes a file path and optional filetype, datatype and tracktype parameters.
    For any parameters that are not provided (None), it will attempt to infer them based on
    the file extension and known mappings between filetypes, datatypes and tracktypes.

    Args:
        file_path (str): Path to the file to analyze
        filetype (str, optional): The type of file (e.g. 'bigwig', 'cooler'). Will be inferred if None.
        datatype (str, optional): The type of data (e.g. 'vector', 'matrix'). Will be inferred if None.
        tracktype (str, optional): The type of track (e.g. 'horizontal-bar', 'heatmap'). Will be inferred if None.

    Returns:
        tuple: A tuple containing (filetype, datatype, tracktype) with all values filled in

    Raises:
        ValueError: If filetype cannot be inferred from the file
    """
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

    if filetype == "gff":
        from clodius.tiles.gff import tileset_info

        # Try getting tileset info to validate that the file is valid
        tileset_info(file_path)

    return filetype, datatype, tracktype


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

    filetype, datatype, tracktype = fill_in_filetype_datatype_tracktype(
        file_path, filetype, datatype, tracktype
    )

    port = _start(
        directory=directory,
        license=None,
        image=image,
        foreground=False,
        platform=platform,
        use_aws_creds=is_s3_file,
    )

    sys.stdout.write("Waiting for server to start...")
    sys.stdout.flush()
    # Wait for server to be ready
    for i in range(30):
        sys.stdout.write(".")
        sys.stdout.flush()

        try:
            response = requests.get(
                f"http://localhost:{port}/api/v1/tilesets/",
                timeout=2,
            )
            if response.status_code == 200:
                break
        except:
            pass
        time.sleep(0.5)

    sys.stdout.write("\n")
    sys.stdout.flush()
    # Sync datasets to ensure file is available
    # _sync_datasets(directory)

    # Create viewconf and open browser
    rgc = rg.connect(
        host=f"http://localhost:{port}",
        auth_provider="local",
        use_dotfile_credentials=False,
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

    if not tileset:
        # need to add the dataset
        if file_path.startswith("s3"):
            uuid = project.add_s3_dataset(file_path)
        else:
            dataset_rel_path = op.relpath(file_path, directory)
            logger.info("Adding link dataset: %s", dataset_rel_path)
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
    url = f"http://localhost:{port}/viewer/{saved_viewconf['uuid']}?at={token['access_token']}&rt={token['refresh_token']}"

    logger.info(f"Opening {url}")
    webbrowser.open(url)
