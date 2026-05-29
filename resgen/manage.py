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
    get_s3_datasets,
    load_s3_mounts,
    save_s3_mounts,
)
from resgen.license import get_license, datasets_allowed, LicenseError
from resgen.exceptions import ResgenError
from resgen.utils import (
    tracktype_default_position,
    datatype_to_tracktype,
    infer_filetype,
    infer_datatype,
)

import shutil
import sys

from slugid import nice

logger = logging.getLogger(__name__)


def get_container_runtime():
    """Get the container runtime to use (docker or finch).

    Checks the RESGEN_CONTAINER_RUNTIME environment variable first.
    If not set, auto-detects by checking which binaries are in PATH,
    preferring docker for backwards compatibility.
    """
    runtime = os.environ.get("RESGEN_CONTAINER_RUNTIME")
    if runtime:
        return runtime

    for candidate in ("docker", "finch"):
        if shutil.which(candidate):
            return candidate

    return "docker"  # fallback default

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
      - RESGEN_HTTP_VERIFY_SSL=false
      - SITE_URL=localhost
      - CELERY_BROKER=rabbitmq:5672
      - RESGEN_LOCAL_JWT_AUTH=True
      - RESGEN_LOCAL_UPLOADS=True
      - SITE_URL=localhost:{port}
      - RESGEN_LICENSE_JWT={resgen_license_jwt}
    container_name: "rgc-{name}"
"""

LOGGED_SERVICES = ["nginx", "uwsgi", "celery"]



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
    # Resolve to absolute path so compose volume paths are absolute and
    # independent of the working directory when docker compose processes the file.
    directory = op.abspath(directory)

    # Resolve license text before checking running containers so we can detect
    # if the license has changed and the container needs a restart
    if not license:
        home_license = os.path.expanduser("~/.resgen/license.jwt")
        if op.exists(home_license):
            logger.info("Using license from ~/.resgen/license.jwt")
            license_text = open(home_license, "r").read()
        else:
            logger.warning(
                "No license file provided, default to guest license. "
                "This will limit the use of this software to a maximum of 20 files "
                "per project."
            )
            license_text = ""
    else:
        license_text = open(license, "r").read()

    # Check if AWS credentials are loaded in existing container
    running_containers = _get_running_containers(image)
    current_container = next(
        (c for c in running_containers if c["directory"] == directory), None
    )

    if current_container:
        import subprocess
        needs_restart = False

        if use_aws_creds:
            # If we need to use aws credentials then we need to make sure
            # they're mounted in the docker container
            try:
                result = subprocess.run(
                    [
                        get_container_runtime(),
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
                    needs_restart = True
            except subprocess.CalledProcessError:
                pass

        if not needs_restart:
            # Check if the license has changed since the container was last started
            cached_license_path = join(directory, ".resgen/license.jwt")
            if op.exists(cached_license_path):
                with open(cached_license_path, "r") as f:
                    cached_license = f.read()
                if cached_license != license_text:
                    logger.info("License has changed, restarting container...")
                    needs_restart = True

        if needs_restart:
            stop(directory)
            current_container = None
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

    cmd = [get_container_runtime(), "compose"]
    cmd += ["--project-name", f"rgc-{directory_hash}"]
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
    directory_hash = hashlib.md5(abspath(directory).encode()).hexdigest()[:8]

    run([get_container_runtime(), "compose", "--project-name", f"rgc-{directory_hash}", "-f", compose_file, "down"])


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

    runtime = get_container_runtime()
    if style == "run":
        run(
            [
                runtime,
                "run",
                "-v",
                f"{directory}/.resgen/data/:/data",
                "-it",
                "resgen-server",
                "bash",
            ]
        )
    else:
        run([runtime, "exec", "-it", f"rgc-{directory_hash}", "bash"])


def _create_user(directory, username, password):
    """Create a resgen user."""
    # print("-c", f"\"import django.contrib.auth; django.contrib.auth.models.User.objects.create_user('{username}', password='{password}')\"")
    compose_file = get_compose_file(directory)
    print("compose_file", compose_file)

    run(
        [
            get_container_runtime(),
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
            get_container_runtime(),
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


def _resolve_license(directory: str):
    """Return the active LicenseInfo for a directory.

    Priority order:
    1. ~/.resgen/license.jwt  (user's license — most authoritative)
    2. <directory>/.resgen/license.jwt  (cached by ``start``)
    3. RESGEN_LICENSE_JWT env var / guest
    """
    home_path = os.path.expanduser("~/.resgen/license.jwt")
    project_path = join(directory, ".resgen/license.jwt")

    if op.exists(home_path):
        lic = get_license(home_path)
        logger.info("License: using %s (permissions=%s)", home_path, lic.permissions)
        return lic
    if op.exists(project_path):
        lic = get_license(project_path)
        logger.info("License: using %s (permissions=%s)", project_path, lic.permissions)
        return lic
    lic = get_license()
    logger.info("License: no file found, using env/guest (permissions=%s)", lic.permissions)
    return lic


def can_sync_datasets(directory, total_tilesets):
    license = _resolve_license(directory)

    # Only guest accounts are limited in how many datasets they
    # can add
    if license.permissions == "guest":

        if total_tilesets > datasets_allowed(license):
            raise LicenseError(
                f"Guest license has exceeded the number of datasets allowed ({datasets_allowed(license)}). "
                "Please go to resgen.io to create a subscription. Any resgen.io subscription "
                "will allow you to view an arbitrary number of datasets."
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
    basename.

    If directory starts with s3://, syncs directly from S3 without local files.
    """
    # Check if this is an S3 path
    is_s3_path = directory.startswith("s3://")

    if is_s3_path:
        # For S3 paths, use the last component as project name
        path_parts = directory.rstrip("/").split("/")
        project_name = path_parts[-1] if len(path_parts) > 1 else path_parts[0]
        # For S3, we need to find a running container in current directory
        working_dir = op.abspath(".")
        host = _get_directory_url(working_dir, image=image)
        if not host:
            logger.error(
                f"No running resgen container found for current directory: {working_dir}. "
                "Start a container first with 'resgen manage start'"
            )
            return
    else:
        directory = op.abspath(directory)
        project_name = op.basename(directory)
        host = _get_directory_url(directory, image=image)

        if not host:
            logger.error(f"No running resgen container found for directory: {directory}")
            return

    user = "local"
    password = "local"

    import time
    import requests as _requests

    for _ in range(60):
        try:
            if _requests.get(f"{host}/api/v1/tilesets/", timeout=2).status_code == 200:
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        logger.error("Server at %s did not become ready in time", host)
        return

    try:
        rgc = rg.connect(
            username=user, password=password, host=host, auth_provider="local"
        )
    except rg.UnknownConnectionException:
        logger.error("Unable to login, please check your username and password")
        return

    project = rgc.find_or_create_project(project_name)

    # Get datasets based on whether this is S3 or local
    if is_s3_path:
        logger.info(f"Syncing from S3 path: {directory}")
        try:
            local_datasets = get_s3_datasets(directory)
        except Exception as e:
            logger.error(f"Failed to list S3 datasets: {e}")
            return
    else:
        local_datasets = get_local_datasets(directory)

        # Load and merge S3 mounts for local directories
        s3_mounts = load_s3_mounts(directory)
        for mount in s3_mounts:
            try:
                logger.info(f"Loading S3 mount: {mount['path']} -> {mount['folder']}")
                s3_datasets = get_s3_datasets(mount["path"], folder_prefix=mount["folder"])
                local_datasets.extend(s3_datasets)
                logger.info(f"Loaded {len(s3_datasets)} datasets from S3 mount")
            except Exception as e:
                logger.error(f"Failed to load S3 mount {mount['path']}: {e}")

    remote_datasets = get_remote_datasets(project)

    # For S3 paths, use current directory for license check
    license_check_dir = working_dir if is_s3_path else directory
    can_sync_datasets(license_check_dir, len(local_datasets))

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


@manage.group()
def s3():
    """Manage S3 mounts for resgen projects."""
    pass


@s3.command("add")
@click.argument("s3_path")
@click.argument("directory", default=".")
@click.option("--folder", default=None, help="Folder name within project (defaults to S3 path basename)")
def s3_add(s3_path, directory, folder):
    """Add an S3 path as a mounted folder in the project.

    Example:
        resgen manage s3 add s3://my-bucket/reference-data
        resgen manage s3 add s3://my-bucket/data --folder refs
        resgen manage s3 add ~/data/project s3://my-bucket/data
    """
    from resgen.sync.folder import load_s3_mounts, save_s3_mounts, get_local_datasets

    directory = op.abspath(directory)

    # Default folder name to last component of S3 path
    if not folder:
        # Parse s3://bucket/prefix to get the last component
        path_parts = s3_path.rstrip("/").split("/")
        folder = path_parts[-1] if len(path_parts) > 1 else path_parts[0]

    # Validate S3 path format
    if not s3_path.startswith("s3://"):
        logger.error("Invalid S3 path. Must start with s3://")
        return

    # Check if folder already exists locally
    local_datasets = get_local_datasets(directory)
    local_folders = {d["name"] for d in local_datasets if d["is_folder"] and "/" not in d["fullpath"]}

    if folder in local_folders:
        logger.error(
            f"Folder '{folder}' already exists locally. "
            "Choose a different name with --folder or rename the local folder."
        )
        return

    # Load existing mounts
    mounts = load_s3_mounts(directory)

    # Check if this folder is already mounted
    if any(m["folder"] == folder for m in mounts):
        logger.error(f"S3 mount with folder '{folder}' already exists")
        return

    # Add new mount
    mounts.append({"path": s3_path, "folder": folder})
    save_s3_mounts(directory, mounts)

    logger.info(f"Added S3 mount: {s3_path} -> {folder}")
    logger.info(f"Run 'resgen manage sync-datasets {directory}' to sync the mounted data")


@s3.command("remove")
@click.argument("folder")
@click.argument("directory", default=".")
def s3_remove(folder, directory):
    """Remove an S3 mount from the project.

    Example:
        resgen manage s3 remove reference-data
        resgen manage s3 remove reference-data ~/data/project
    """
    from resgen.sync.folder import load_s3_mounts, save_s3_mounts

    directory = op.abspath(directory)

    # Load existing mounts
    mounts = load_s3_mounts(directory)

    # Find and remove the mount
    original_count = len(mounts)
    mounts = [m for m in mounts if m["folder"] != folder]

    if len(mounts) == original_count:
        logger.error(f"No S3 mount found with folder '{folder}'")
        return

    save_s3_mounts(directory, mounts)
    logger.info(f"Removed S3 mount: {folder}")
    logger.info(f"Run 'resgen manage sync-datasets {directory}' to remove the remote datasets")


@s3.command("list")
@click.argument("directory", default=".")
def s3_list(directory):
    """List all S3 mounts for the project.

    Example:
        resgen manage s3 list
        resgen manage s3 list ~/data/project
    """
    from resgen.sync.folder import load_s3_mounts

    directory = op.abspath(directory)
    mounts = load_s3_mounts(directory)

    if not mounts:
        print("No S3 mounts configured.")
        return

    print(f"{'Folder':<30} {'S3 Path':<60}")
    print("-" * 90)
    for mount in mounts:
        print(f"{mount['folder']:<30} {mount['path']:<60}")


def _get_running_containers(image=DEFAULT_IMAGE):
    """Get running resgen containers data."""
    import subprocess
    import json

    try:
        runtime = get_container_runtime()
        result = subprocess.run(
            [
                runtime,
                "ps",
                "--filter",
                "name=rgc-",
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
                    [runtime, "inspect", container["ID"]],
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

    print(f"{'URL':<25} {'Status':<20} {'Container ID':<15} {'Directory':<50}")
    print("-" * 112)

    for container in containers:
        url = f"http://localhost:{container['port']}"
        print(f"{url:<25} {'Running':<20} {container['id'][:12]:<15} {container['directory']:<50}\n")


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

    directory = op.abspath(directory)
    url = _get_directory_url(directory)

    if not url:
        logger.error(f"No running resgen container found for directory: {directory}")
        return

    rgc = rg.connect(
        host=url,
        auth_provider="local",
        credentials_dir=directory,
    )

    token = rgc.get_local_token()
    url = f"{url}?at={token['access_token']}&rt={token['refresh_token']}"

    logger.info(f"Opening {url}")
    webbrowser.open(url)


@manage.command()
@click.option("-i", "--image", default=DEFAULT_IMAGE)
def update(image):
    """Pull the latest resgen image.

    Does the same thing as the "pull" command below
    """
    run([get_container_runtime(), "pull", image])
    logger.info(f"Updated image: {image}")


@manage.command()
@click.option("-i", "--image", default=DEFAULT_IMAGE)
def pull(image):
    """Pull the latest resgen image.

    Alias of "update"
    """
    run([get_container_runtime(), "pull", image])
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
        credentials_dir=directory,
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
        try:
            if file_path.startswith("s3"):
                uuid = project.add_s3_dataset(file_path)
            else:
                dataset_rel_path = op.relpath(file_path, directory)
                logger.info("Adding link dataset: %s", dataset_rel_path)
                uuid = project.add_link_dataset(dataset_rel_path)
        except ResgenError as e:
            raise click.ClickException(str(e))

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


@manage.command()
@click.argument("files", nargs=-1, required=True)
@click.option(
    "-ref",
    "--reference",
    required=True,
    help="Reference FASTA file to align sequences against",
)
@click.option("-t", "--tag", multiple=True, help="Pass in tags (e.g. colname:sequence)")
@click.option("-th", "--track-height", default=100)
@click.option("--image", default=DEFAULT_IMAGE)
@click.option("--platform", default=None)
def pileup(files, reference, tag, track_height, image, platform):
    """Align sequences in one or more CSV files against a reference FASTA and display as a pileup."""
    import math
    import webbrowser
    import time
    import requests

    csv_paths = [op.abspath(f) for f in files]
    fasta_path = op.abspath(reference)

    if not op.isfile(fasta_path):
        raise click.ClickException(f"Reference file not found: {fasta_path}")

    for csv_path in csv_paths:
        if csv_path == fasta_path:
            raise click.ClickException(
                "The reference file and the pileup file cannot be the same file"
            )

    tag_names = [t.split(":")[0] for t in tag]
    if "colname" not in tag_names and "colnum" not in tag_names:
        raise click.ClickException(
            "A column must be specified with -t colname:<name> or -t colnum:<number>"
        )

    # Validate that the specified column exists in each CSV file
    import csv as csv_module

    colname_tag = next((t for t in tag if t.startswith("colname:")), None)
    colnum_tag = next((t for t in tag if t.startswith("colnum:")), None)
    header_tag = next((t for t in tag if t.startswith("header:")), None)
    has_header = header_tag is None or header_tag.split(":", 1)[1].lower() != "false"

    for csv_path in csv_paths:
        try:
            with open(csv_path, "r") as f:
                first_row = next(csv_module.reader(f), None)

            if first_row is None:
                raise click.ClickException(f"CSV file is empty: {csv_path}")

            if colname_tag and has_header:
                colname = colname_tag.split(":", 1)[1]
                if colname not in first_row:
                    raise click.ClickException(
                        f"Column '{colname}' not found in {op.basename(csv_path)}. "
                        f"Available columns: {', '.join(first_row)}"
                    )
            elif colnum_tag:
                colnum = int(colnum_tag.split(":", 1)[1])
                if colnum < 1 or colnum > len(first_row):
                    raise click.ClickException(
                        f"Column number {colnum} is out of range: file has {len(first_row)} column(s)"
                    )
        except (IOError, OSError) as e:
            raise click.ClickException(f"Could not read CSV file: {e}")

    # Derive assembly name from the FASTA filename stem (e.g. "ref.fa" -> "ref")
    assembly_name = op.splitext(op.basename(fasta_path))[0]

    # Determine the directory to mount in Docker (common ancestor of all files)
    common = op.commonpath([*csv_paths, fasta_path])
    directory = common if op.isdir(common) else op.dirname(common)

    for csv_path in csv_paths:
        logger.info("csv_path %s", csv_path)
    logger.info("fasta_path %s", fasta_path)
    logger.info("assembly_name %s", assembly_name)
    logger.info("directory %s", directory)

    port = _start(
        directory=directory,
        license=None,
        image=image,
        foreground=False,
        platform=platform,
        use_aws_creds=False,
    )

    sys.stdout.write("Waiting for server to start...")
    sys.stdout.flush()
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

    rgc = rg.connect(
        host=f"http://localhost:{port}",
        auth_provider="local",
        credentials_dir=directory,
    )

    project = rgc.find_or_create_project(op.basename(directory))
    token = rgc.get_local_token()

    def find_or_add_tileset(file_path):
        """Find an existing tileset by filename in the project, or add it."""
        filename = op.basename(file_path)
        for ts in project.list_datasets():
            if filename in ts.datafile:
                return rgc.get_dataset(ts.uuid)
        rel_path = op.relpath(file_path, directory)
        try:
            uuid = project.add_link_dataset(rel_path)
        except ResgenError as e:
            raise click.ClickException(str(e))
        return rgc.get_dataset(uuid)

    # Generate .fai index if needed before registering the FASTA tileset, so
    # we can include the indexfile in the same update call as the tags.
    # TilesetSerializer.update removes any tag not present in the PATCH body,
    # so two separate calls would wipe the tags set by the first call.
    fai_path = fasta_path + ".fai"
    if not op.isfile(fai_path):
        import subprocess

        try:
            subprocess.run(
                ["samtools", "faidx", fasta_path],
                check=True,
                capture_output=True,
            )
            logger.info("Generated .fai index for %s", fasta_path)
        except subprocess.CalledProcessError as e:
            logger.warning("Could not generate .fai index: %s", e)
        except FileNotFoundError:
            logger.warning("samtools not found; chromsizes and sequence tracks will be skipped")

    # Register FASTA tileset — include indexfile in the same call as tags so
    # the serializer doesn't clear the tags when setting indexfile.
    fasta_tileset = find_or_add_tileset(fasta_path)
    fasta_update = {
        "tags": [
            {"name": "filetype:fasta_seq"},
            {"name": "datatype:sequence"},
            {"name": f"assembly:{assembly_name}"},
        ]
    }
    if op.isfile(fai_path):
        fasta_update["indexfile"] = op.relpath(fai_path, directory)
    rgc.update_dataset(fasta_tileset.uuid, fasta_update)

    chromsizes_tileset = None
    if op.isfile(fai_path):
        rel_fai_path = op.relpath(fai_path, directory)

        # Try to register the .fai as a separate chromsizes-tsv tileset so that
        # the horizontal-chromosome-labels track can use it.  This may fail when
        # the guest-license dataset limit is already reached; in that case we
        # skip the chromosome-labels track but the sequence track still works
        # because the indexfile is already set above.
        fai_filename = op.basename(fai_path)
        try:
            existing = next(
                (ts for ts in project.list_datasets() if fai_filename in ts.datafile),
                None,
            )
            if existing:
                chromsizes_tileset = rgc.get_dataset(existing.uuid)
            else:
                uuid = project.add_link_dataset(rel_fai_path)
                chromsizes_tileset = rgc.get_dataset(uuid)

            rgc.update_dataset(
                chromsizes_tileset.uuid,
                {
                    "tags": [
                        {"name": "filetype:chromsizes-tsv"},
                        {"name": "datatype:chromsizes"},
                        {"name": f"assembly:{assembly_name}"},
                    ]
                },
            )
        except ResgenError as e:
            logger.warning("Could not register chromsizes tileset: %s; chromosome-labels track will be skipped", e)

    from higlass import view as hg_view

    # Build common track prefix (chromosome labels + sequence) shared across all views
    def _make_common_tracks():
        tracks = []
        if chromsizes_tileset:
            tracks.append(
                chromsizes_tileset.hg_track(
                    track_type="horizontal-chromosome-labels", position="top", height=20
                )
            )
        if op.isfile(fai_path):
            tracks.append(
                fasta_tileset.hg_track(
                    track_type="horizontal-sequence", position="top", height=40
                )
            )
        return tracks

    # Compute grid dimensions approximating the golden ratio (ncols / nrows ≈ φ)
    phi = (1 + math.sqrt(5)) / 2
    def _grid_dims(n):
        if n == 1:
            return 1, 1
        best = (1, 1, float("inf"))
        for nrows in range(1, math.ceil(math.sqrt(n)) + 1):
            ncols = math.ceil(n / nrows)
            if ncols < nrows:
                continue
            dist = abs(ncols / nrows - phi)
            if dist < best[2]:
                best = (ncols, nrows, dist)
        return best[0], best[1]

    ncols, nrows = _grid_dims(len(csv_paths))
    view_width = max(1, 12 // ncols)

    # Register each CSV tileset and build its view
    all_views = []
    for csv_path in csv_paths:
        csv_tileset = find_or_add_tileset(csv_path)
        csv_tags = [
            {"name": "filetype:pileup-csv"},
            {"name": "datatype:reads"},
            {"name": f"assembly:{assembly_name}"},
        ]
        for t in tag:
            csv_tags.append({"name": t})
        rgc.update_dataset(csv_tileset.uuid, {"tags": csv_tags})

        pileup_track = csv_tileset.hg_track(
            track_type="pileup",
            position="top",
            height=track_height,
            name=op.basename(csv_path),
            labelPosition="bottomLeft",
            labelColor="#808080",
        )
        tracks = _make_common_tracks() + [pileup_track]
        all_views.append(hg_view(*tracks, width=view_width))

    # Arrange views into rows, then stack rows vertically
    row_viewconfs = []
    for row_idx in range(nrows):
        start = row_idx * ncols
        row_views = all_views[start : start + ncols]
        row_vc = row_views[0]
        for v in row_views[1:]:
            row_vc = row_vc | v
        row_viewconfs.append(row_vc)

    viewconf = row_viewconfs[0]
    for row_vc in row_viewconfs[1:]:
        viewconf = viewconf / row_vc

    # hg_view() returns a View (has .viewconf()); | and / return a Viewconf directly
    vc = viewconf.viewconf() if hasattr(viewconf, "viewconf") else viewconf
    title = "Pileup view" if len(csv_paths) == 1 else f"Pileup views ({len(csv_paths)} files)"
    saved_viewconf = project.add_viewconf(vc.dict(), title)
    url = f"http://localhost:{port}/viewer/{saved_viewconf['uuid']}?at={token['access_token']}&rt={token['refresh_token']}"

    logger.info(f"Opening {url}")
    webbrowser.open(url)
