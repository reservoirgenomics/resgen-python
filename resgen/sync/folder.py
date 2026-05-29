import os.path as op
import os

import logging

logger = logging.getLogger(__name__)


def get_s3_datasets(s3_path, folder_prefix=""):
    """Get a list of all datasets from an S3 path.

    Args:
        s3_path: S3 path in format s3://bucket/prefix
        folder_prefix: Optional folder prefix to prepend to all paths

    Returns:
        List of dataset dicts with same structure as get_local_datasets
    """
    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError
    except ImportError:
        raise ImportError(
            "boto3 is required for S3 operations. Install it with: pip install boto3"
        )

    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}. Must start with s3://")

    # Parse s3://bucket/prefix
    path_parts = s3_path[5:].split("/", 1)
    bucket = path_parts[0]
    prefix = path_parts[1] if len(path_parts) > 1 else ""

    # Ensure prefix ends with / if non-empty (for proper listing)
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    s3_client = boto3.client("s3")
    datasets = []
    folders_seen = set()

    try:
        # List all objects with the given prefix
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]

                # Skip if this is just the prefix itself (directory marker)
                if key == prefix:
                    continue

                # Get relative path from the prefix
                rel_path = key[len(prefix):]

                # Add folder prefix if specified
                if folder_prefix:
                    full_path = op.join(folder_prefix, rel_path)
                else:
                    full_path = rel_path

                # Create folder entries for all parent directories
                path_parts = rel_path.split("/")
                for i in range(len(path_parts) - 1):
                    folder_rel_path = "/".join(path_parts[:i+1])
                    if folder_prefix:
                        folder_full_path = op.join(folder_prefix, folder_rel_path)
                    else:
                        folder_full_path = folder_rel_path

                    if folder_full_path not in folders_seen:
                        folders_seen.add(folder_full_path)
                        datasets.append({
                            "fullpath": folder_full_path,
                            "name": path_parts[i],
                            "is_folder": True,
                            "is_s3": True,
                        })

                # Add the file itself
                datasets.append({
                    "fullpath": full_path,
                    "name": op.basename(rel_path),
                    "is_folder": False,
                    "is_s3": True,
                    "s3_uri": f"s3://{bucket}/{key}",
                })

    except NoCredentialsError:
        raise RuntimeError(
            "AWS credentials not found. Configure credentials using AWS CLI or environment variables."
        )
    except ClientError as e:
        raise RuntimeError(f"Failed to list S3 objects: {e}")

    # Associate index files (same logic as local datasets)
    by_fullpath = dict([(d["fullpath"], d) for d in datasets])
    to_remove = set()

    for d in datasets:
        if d["is_folder"]:
            continue

        for index_extension in ["bai", "fai", "tbi"]:
            index_path = f"{d['fullpath']}.{index_extension}"
            if index_path in by_fullpath:
                # Store the S3 URI for the index file
                d["index_filepath"] = by_fullpath[index_path]["s3_uri"]
                if index_extension in ["bai", "tbi"]:
                    to_remove.add(index_path)

    datasets = [d for d in datasets if d["fullpath"] not in to_remove]

    return datasets


def generate_fai_index_if_needed(dataset, directory):
    """Generate .fai index for FASTA files if needed and update dataset."""
    fai_path = f"{dataset['fullpath']}.fai"
    full_fai_path = op.join(directory, fai_path)
    if not op.exists(full_fai_path):
        import subprocess

        try:
            subprocess.run(
                ["samtools", "faidx", op.join(directory, dataset["fullpath"])],
                check=True,
            )
            logger.info(f"Generated .fai index for {dataset['fullpath']}")
        except subprocess.CalledProcessError as e:
            logger.warning(
                f"Failed to generate .fai index for {dataset['fullpath']}: {e}"
            )
        except FileNotFoundError:
            logger.warning("samtools not found, cannot generate .fai index")

    # Check again if the .fai file exists (either was there or just created)
    if op.exists(full_fai_path):
        dataset["index_filepath"] = fai_path


def get_local_datasets(directory):
    """Get a list of all local datasets within the directory."""
    local_datasets = []

    for path, folders, files in os.walk(directory):
        if path.startswith(op.join(directory, ".resgen")):
            continue

        for folder in folders:
            full_folder = op.join(path, folder)
            if full_folder.startswith(op.join(directory, ".resgen")):
                # skip the .resgen metadata directory
                continue

            local_datasets += [
                {
                    "fullpath": op.relpath(full_folder, directory),
                    "name": folder,
                    "is_folder": True,
                }
            ]
        for file in files:
            full_file = op.join(path, file)

            local_datasets += [
                {
                    "fullpath": op.relpath(full_file, directory),
                    "name": file,
                    "is_folder": False,
                }
            ]

    # We'll go through and associate indexfiles
    by_fullpath = dict([(d["fullpath"], d) for d in local_datasets])
    to_remove = set()

    for d in local_datasets:
        for index_extension in ["bai", "fai", "tbi"]:
            index_path = f"{d['fullpath']}.{index_extension}"
            if index_path in by_fullpath:
                d["index_filepath"] = index_path
                if index_extension in ["bai", "tbi"]:
                    # We have no use for .bai files outside of as indexes
                    # .fai files we can add as chromsizes
                    to_remove.add(index_path)

        # Check if FASTA files (.fa, .fna) need .fai index generation
        if d["fullpath"].endswith((".fa", ".fna", ".fasta")) and not d.get(
            "index_filepath"
        ):
            generate_fai_index_if_needed(d, directory)

    local_datasets = [d for d in local_datasets if d["fullpath"] not in to_remove]

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
            "fullname": ds.name,
            "index_filepath": ds.indexfile,
        }

        while ds.containing_folder:
            ds1 = ds_by_uid[ds.containing_folder]
            filename = op.join(ds1.name, filename)

            ds = ds1

        ds_json["fullname"] = filename
        remote_datasets += [ds_json]

    return dict([(ds["fullname"], ds) for ds in remote_datasets])


def remove_stale_remote_datasets(project, local_datasets, remote_datasets):
    """Remove any remote datasets which are not reflected in the local
    datasets."""
    local_fullpath = dict([(d["fullpath"], d) for d in local_datasets])

    for remote_fullpath, ds in remote_datasets.items():
        if remote_fullpath not in local_fullpath:
            logger.info("Removing stale remote dataset: %s", remote_fullpath)
            project.delete_dataset(ds["uuid"])


def load_s3_mounts(directory):
    """Load S3 mount configuration from .resgen/mounts.yml

    Args:
        directory: Base directory containing .resgen/mounts.yml

    Returns:
        List of mount dicts with 'path' and 'folder' keys, or empty list if no config
    """
    mounts_file = op.join(directory, ".resgen/mounts.yml")
    if not op.exists(mounts_file):
        return []

    try:
        import yaml
    except ImportError:
        logger.warning("PyYAML not installed, cannot load S3 mounts")
        return []

    try:
        with open(mounts_file, "r") as f:
            config = yaml.safe_load(f) or {}
        return config.get("s3_mounts", [])
    except Exception as e:
        logger.error(f"Failed to load S3 mounts from {mounts_file}: {e}")
        return []


def save_s3_mounts(directory, mounts):
    """Save S3 mount configuration to .resgen/mounts.yml

    Args:
        directory: Base directory to save .resgen/mounts.yml to
        mounts: List of mount dicts with 'path' and 'folder' keys
    """
    try:
        import yaml
    except ImportError:
        raise ImportError(
            "PyYAML is required for S3 mount management. Install it with: pip install pyyaml"
        )

    resgen_dir = op.join(directory, ".resgen")
    if not op.exists(resgen_dir):
        os.makedirs(resgen_dir)

    mounts_file = op.join(resgen_dir, "mounts.yml")
    config = {"s3_mounts": mounts}

    try:
        with open(mounts_file, "w") as f:
            yaml.dump(config, f, default_flow_style=False)
    except Exception as e:
        raise RuntimeError(f"Failed to save S3 mounts to {mounts_file}: {e}")


def add_and_update_local_datasets(
    project, local_datasets, remote_datasets, base_directory, link=True
):
    """Add local datasets to remote if they're missing.

    :param base_directory: The base directory to which all local filepaths are
        relative.
    :param link: Add as links rather than uploading. Useful if running a local
        version of resgen.
    """

    def get_parent_uuid(dataset):
        parent_dir = op.split(dataset["fullpath"])[0]
        parent = remote_datasets.get(parent_dir)
        if parent:
            parent = parent["uuid"]

        return parent

    for dataset in local_datasets:
        if dataset["fullpath"] in remote_datasets:
            # see if we have to update it
            rd = remote_datasets[dataset["fullpath"]]
            if rd.get("index_filepath") != dataset.get("index_filepath"):
                logger.info(
                    "Updating indexfile for %s with %s",
                    dataset["fullpath"],
                    dataset.get("index_file"),
                )
                project.conn.update_dataset(
                    rd["uuid"], {"indexfile": dataset.get("index_filepath")}
                )
        else:
            if dataset["is_folder"]:
                parent = get_parent_uuid(dataset)
                uuid = project.add_folder_dataset(
                    folder_name=dataset["name"], parent=parent
                )
                remote_datasets[dataset["fullpath"]] = {"uuid": uuid, "is_folder": True}
            else:
                # Handle adding a file dataset
                parent = get_parent_uuid(dataset)
                logger.info(
                    "Adding dataset name: %s datafile: %s, parent: %s",
                    dataset["name"],
                    dataset["fullpath"],
                    parent,
                )

                # Check if this is an S3 dataset
                if dataset.get("is_s3"):
                    uuid = project.add_s3_dataset(
                        filepath=dataset["s3_uri"],
                        index_filepath=dataset.get("index_filepath"),
                        name=dataset["name"],
                        parent=parent,
                        private=False,
                    )
                elif link:
                    uuid = project.add_link_dataset(
                        filepath=dataset["fullpath"],
                        index_filepath=dataset.get("index_filepath"),
                        name=dataset["name"],
                        parent=parent,
                        private=False,
                    )
                else:
                    uuid = project.add_upload_dataset(
                        filepath=op.join(base_directory, dataset["fullpath"]),
                        index_filepath=dataset.get("index_filepath")
                        and op.join(base_directory, dataset.get("index_filepath")),
                        name=dataset["name"],
                        parent=parent,
                        private=False,
                    )

                logger.info("Added with uuid: %s", uuid)
                remote_datasets[dataset["fullpath"]] = {
                    "uuid": uuid,
                    "is_folder": False,
                }
