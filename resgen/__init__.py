import base64
import json
import logging
import os.path as op
import requests
import slugid
import sys
import tempfile
import time
import typing

import higlass.client as hgc
from higlass import Track
from resgen import aws

# import resgen.utils as rgu

logger = logging.getLogger(__name__)

__version__ = "0.2.1"

RESGEN_HOST = "https://resgen.io"
RESGEN_BUCKET = "resgen"

# RESGEN_HOST = "http://localhost:8000"
# RESGEN_BUCKET = "resgen-test"


def update_tags(current_tags, **kwargs):
    """Update a list of tags to use for searching.

    If a tag type (e.g. 'datatype') that is already in current_tags
    is specified as part of kwargs, then it overwrites the one in
    current_tags.
    """
    for tag in kwargs:
        current_tags = [t for t in current_tags if not t["name"].startwith(f"{tag}:")]
        current_tags += [{"name": f"{tag}:{kwargs[tag]}"}]

    return current_tags


def tags_to_datatype(tags):
    """Extract a datatype from a set of tags"""
    for tag in tags:
        if tag["name"].startswith("datatype:"):
            return tag["name"].split(":")[1]

    return None


class InvalidCredentialsException(Exception):
    """Raised when invalid credentials are passed in."""

    pass


class UnknownConnectionException(Exception):
    """Some error occurred when trying to perform a network operation."""

    def __init__(self, message, request_return):
        super().__init__(
            f"{message}: "
            f"status_code: {request_return.status_code}, "
            f"content: {request_return.content}"
        )


class ChromosomeInfo:
    def __init__(self):
        self.total_length = 0
        self.cum_chrom_lengths = {}
        self.chrom_lengths = {}
        self.chrom_order = []

    def to_abs(self, chrom, pos):
        """Calculate absolute coordinates."""
        return self.cum_chrom_lengths[chrom] + pos


def get_chrominfo_from_string(chromsizes_str):
    chrom_info = ChromosomeInfo()
    total_length = 0

    for line in chromsizes_str.strip("\n").split("\n"):
        rec = line.split()
        total_length += int(rec[1])

        chrom_info.cum_chrom_lengths[rec[0]] = total_length - int(rec[1])
        chrom_info.chrom_lengths[rec[0]] = int(rec[1])
        chrom_info.chrom_order += [rec[0]]

    chrom_info.total_length = total_length
    return chrom_info


class ResgenDataset:
    """Encapsulation of a resgen dataset. Typically initialized
    from the return of a dataset search or sync on the server."""

    def __init__(self, conn, data):
        """Initialize with data returned from the tileset server."""
        self.data = data

        self.conn = conn

        self.uuid = data["uuid"]
        self.tags = data["tags"]
        self.name = data["name"]

    def __str__(self):
        """String representation."""
        return f"{self.uuid[:8]}: {self.name}"

    def __repr__(self):
        """String representation."""
        return f"{self.uuid[:8]}: {self.name}"

    def hg_track(
        self, track_type=None, position=None, height=None, width=None, **options
    ):
        """Create a higlass track from this dataset."""
        datatype = tags_to_datatype(self.tags)

        if track_type is None:
            track_type, position = hgc.datatype_to_tracktype(datatype)
        else:
            if position is None:
                position = hgc.tracktype_default_position(track_type)
            if position is None:
                raise ValueError(
                    f"No default position for track type: {track_type}. "
                    "Please specify a position"
                )

        return Track(
            track_type,
            position,
            height=height,
            width=width,
            tileset_uuid=self.uuid,
            server=f"{self.conn.host}/api/v1",
            options=options,
        )


class ResgenConnection:
    """Connection to the resgen server."""

    def __init__(self, username, password, host=RESGEN_HOST, bucket=RESGEN_BUCKET):
        self.username = username
        self.password = password
        self.host = host
        self.bucket = bucket

        self.token = None
        self.token = self.get_token()

    def authenticated_request(self, func, *args, **kwargs):
        """Send post request"""
        token = self.get_token()

        return func(
            *args, **{**kwargs, "headers": {"Authorization": "JWT {}".format(token)}}
        )

    def get_token(self) -> str:
        """Get a JWT token for interacting with the service."""
        if self.token:
            ret = requests.get(
                f"{self.host}/api/v1/which_user/",
                headers={"Authorization": "JWT {}".format(self.token)},
            )

            if ret.status_code == 200:
                return self.token

        ret = requests.post(
            f"{self.host}/api-token-auth/",
            data={"username": self.username, "password": self.password},
        )

        if ret.status_code == 400:
            raise InvalidCredentialsException(
                "The provided username and password are incorrect"
            )
        elif ret.status_code != 200:
            raise UnknownConnectionException("Failed to login", ret)

        data = json.loads(ret.content.decode("utf8"))
        self.token = data["token"]
        return self.token

    def find_or_create_project(self, project_name: str, private: bool = True):
        """Create a project.

        For now this function can only create a project for the
        logged in user. If a project with the same name exists, do
        nothing.

        Args:
            project_name: The name of the project to create.
            private: Whether to make this a private project.
        """
        ret = self.authenticated_request(
            requests.post,
            f"{self.host}/api/v1/projects/",
            json={"name": project_name, "private": private, "tilesets": []},
        )

        if ret.status_code == 409 or ret.status_code == 201:
            content = json.loads(ret.content)

            return ResgenProject(content["uuid"], self)

        raise UnknownConnectionException("Failed to create project", ret)

    def get_dataset(self, uuid):
        """Retrieve a dataset."""
        url = f"{self.host}/api/v1/tilesets/{uuid}/"

        ret = self.authenticated_request(requests.get, url)

        if ret.status_code != 200:
            raise UnknownConnectionException("Unable to get dataset", ret)

        return json.loads(ret.content)

    def find_datasets(self, search_string="", project=None, limit=10, **kwargs):
        """Search for datasets."""
        tags_line = "&".join(
            [
                f"t={k}:{v}"
                for k, v in kwargs.items()
                if k not in ["search_string", "project", "limit"]
            ]
        )

        url = f"{self.host}/api/v1/list_tilesets/?x=y&{tags_line}"
        if project:
            url += f"&ui={project.uuid}"

        url += f"&ac={search_string}&limit={limit}"
        ret = self.authenticated_request(requests.get, url)

        if ret.status_code == 200:
            content = json.loads(ret.content)

            return [ResgenDataset(self, c) for c in content["results"]]

        raise UnknownConnectionException("Failed to retrieve tilesets", ret)

    def get_genes(self, annotations_ds, gene_name):
        """Retreive gene information by searching by gene name."""
        url = f"{self.host}/api/v1/suggest/?d={annotations_ds.uuid}&ac={gene_name}"

        ret = self.authenticated_request(requests.get, url)

        if ret.status_code != 200:
            raise UnknownConnectionException("Failed to retrieve genes", ret)

        suggestions = json.loads(ret.content)
        return suggestions

    def get_chrominfo(self, chrominfo_ds):
        """Retrieve chromosome information from a chromsizes dataset."""
        url = f"{self.host}/api/v1/chrom-sizes/?id={chrominfo_ds.uuid}"
        ret = self.authenticated_request(requests.get, url)

        if ret.status_code != 200:
            raise UnknownConnectionException("Failed to retrieve chrominfo", ret)

        return get_chrominfo_from_string(ret.content.decode("utf8"))

    def download_progress(self, tileset_uuid):
        """Get the download progress for a tileset.

        Raise an exception if there's no recorded tileset
        progress for this uuid."""
        url = f"{self.host}/api/v1/download_progress/?d={tileset_uuid}"

        ret = self.authenticated_request(requests.get, url)

        if ret.status_code != 200:
            raise UnknownConnectionException(
                "Failed to retrieve download progress", ret
            )

        return json.loads(ret.content)


class ResgenProject:
    """Encapsulates a project on the resgen service."""

    def __init__(self, uuid: str, conn: ResgenConnection):
        """Initialize the project object."""
        self.uuid = uuid
        self.conn = conn

    def list_datasets(self, limit: int = 100):
        """List the datasets available in this project.

        Returns up to a limit
        """
        url = f"{self.conn.host}/api/v1/list_tilesets/?ui={self.uuid}&offset=0"
        ret = self.conn.authenticated_request(requests.get, url)

        if ret.status_code != 200:
            raise UnknownConnectionException("Failed to retrieve tilesets", ret)

        return json.loads(ret.content)["results"]

        # raise NotImplementedError()

    def upload_to_resgen_aws(self, filepath, prefix=None):
        """Upload file to a resgen aws bucket."""
        logger.info("Getting upload credentials for file: %s", filepath)
        url = f"{self.conn.host}/api/v1/prepare_file_upload/"
        if prefix:
            url = f"{url}/?d={prefix}"
        ret = self.conn.authenticated_request(requests.get, url)

        if ret.status_code != 200:
            raise UnknownConnectionException("Failed to prepare file upload", ret)

        content = json.loads(ret.content)
        filename = op.split(filepath)[1]
        directory_path = f"{content['fileDirectory']}/{filename}"
        object_name = f"{content['uploadBucketPrefix']}/{filename}"

        logger.info("Uploading to aws object: %s", object_name)

        bucket = self.conn.bucket
        if aws.upload_file(filepath, bucket, content, object_name):
            return directory_path

        return None

    def add_dataset(self, filepath: str, download: bool = False):
        """Add a dataset

        Args:
            filepath: The filename of the dataset to add. Can also be a url.
            download: If the filepath is a url, download it and save it to our
                datastore. Useful for files on ftp servers or servers that do
                not have range requests.

        Returns:
            The uuid of the newly created dataset.

        """
        if download:
            ret = self.conn.authenticated_request(
                requests.post,
                f"{self.conn.host}/api/v1/tilesets/",
                json={
                    "datafile": filepath,
                    "private": True,
                    "project": self.uuid,
                    "description": f"Downloaded from {filepath}",
                    "download": True,
                    "tags": [],
                },
            )

            content = json.loads(ret.content)

            progress = {"downloaded": 0, "uploaded": 0, "filesize": 1}
            while (
                progress["downloaded"] < progress["filesize"]
                or progress["uploaded"] < progress["filesize"]
                or progress["downloaded"] == 0
            ):
                try:
                    progress = self.conn.download_progress(content["uuid"])
                except UnknownConnectionException:
                    pass
                time.sleep(0.5)

                if progress["filesize"] > 0:
                    percent_done = (
                        100
                        * (progress["downloaded"] + progress["uploaded"])
                        / (2 * progress["filesize"])
                    )

                    sys.stdout.write(f"\r {percent_done:.2f}% Complete")

            return content["uuid"]

        directory_path = self.upload_to_resgen_aws(filepath)

        logger.info("Adding tileset entry for uploaded file: %s", directory_path)

        ret = self.conn.authenticated_request(
            requests.post,
            f"{self.conn.host}/api/v1/finish_file_upload/",
            json={"filepath": directory_path, "project": self.uuid},
        )

        if ret.status_code != 200:
            raise UnknownConnectionException("Failed to finish uploading file", ret)

        print("content:", ret.content)
        content = json.loads(ret.content)
        return content["uuid"]

    def update_dataset(self, uuid: str, metadata: typing.Dict[str, typing.Any]):
        """Update the properties of a dataset."""
        new_metadata = {}

        if "name" in metadata:
            new_metadata["name"] = metadata["name"]
        if "tags" in metadata:
            new_metadata["tags"] = metadata["tags"]

        ret = self.conn.authenticated_request(
            requests.patch,
            f"{self.conn.host}/api/v1/tilesets/{uuid}/",
            json=new_metadata,
        )

        if ret.status_code != 202:
            raise UnknownConnectionException("Failed to update dataset", ret)

        return uuid

    def delete_dataset(self, uuid: str):
        """Delete a dataset."""
        ret = self.conn.authenticated_request(
            requests.delete, f"{self.conn.host}/api/v1/tilesets/{uuid}/"
        )

        if ret.status_code != 204:
            raise UnknownConnectionException("Failed to delete dataset", ret)

        return uuid

    def save_viewconf(self, viewconf, name):
        """Save a viewconf to this project."""
        viewconf_str = json.dumps(viewconf)

        post_data = {
            "viewconf": viewconf_str,
            "project": self.uuid,
            "name": name,
            "visible": True,
            "uid": slugid.nice(),
        }

        ret = self.conn.authenticated_request(
            requests.post, f"{self.conn.host}/api/v1/viewconfs/", json=post_data
        )

        print("ret:", ret)

    def sync_dataset(
        self,
        filepath: str,
        filetype=None,
        datatype=None,
        assembly=None,
        metadata: typing.Dict[str, typing.Any] = {},
    ):
        """Check if this file already exists in this dataset.

        Do nothing if it does and create it if it doesn't. If a new
        dataset is created.

        In both instances, ensure that the metadata is updated. The available
        metadata tags that can be updated are: `name` and `tags`

        If more than one dataset with this name exists, raise a ValueError.

        Args:
        """
        if (
            filepath.startswith("http://")
            or filepath.startswith("https://")
            or filepath.startswith("ftp://")
        ):
            download = True
        else:
            download = False

        datasets = self.list_datasets()
        filename = op.split(filepath)[1]

        def ds_filename(dataset):
            """Return just the filename of a dataset."""
            filename = op.split(dataset["datafile"])[1]
            return filename

        matching_datasets = [d for d in datasets if ds_filename(d) == filename]

        if len(matching_datasets) > 1:
            raise ValueError("More than one matching dataset")

        if not matching_datasets:
            uuid = self.add_dataset(filepath, download=download)
        else:
            uuid = matching_datasets[0]["uuid"]

        to_update = {"tags": []}
        if "name" in metadata:
            to_update["name"] = metadata["name"]
        if "tags" in metadata:
            to_update["tags"] = metadata["tags"]

        if filetype:
            to_update["tags"] = [
                t for t in to_update["tags"] if not t["name"].startswith("filetype:")
            ]
            to_update["tags"] += [{"name": f"filetype:{filetype}"}]
        if datatype:
            to_update["tags"] = [
                t for t in to_update["tags"] if not t["name"].startswith("datatype:")
            ]
            to_update["tags"] += [{"name": f"datatype:{datatype}"}]
        if assembly:
            to_update["tags"] = [
                t for t in to_update["tags"] if not t["name"].startswith("assembly:")
            ]
            to_update["tags"] += [{"name": f"assembly:{assembly}"}]

        return self.update_dataset(uuid, to_update)


def connect(username: str, password: str, host: str = RESGEN_HOST) -> ResgenConnection:
    """Obtain a connection to resgen."""
    return ResgenConnection(username, password, host)
