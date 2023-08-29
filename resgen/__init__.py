import base64
import getpass
import json
import logging
import os
import os.path as op
import re
import sys
import tempfile
import time
import typing
from pathlib import Path

import requests
from dotenv import load_dotenv

from higlass.api import Viewconf

import slugid
from higlass import track

# from higlass.utils import fill_filetype_and_datatype
from resgen import aws
from resgen.utils import tracktype_default_position, datatype_to_tracktype, infer_filetype, infer_datatype

# import resgen.utils as rgu
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

__version__ = "0.6.1"

RESGEN_HOST = "https://resgen.io"
RESGEN_BUCKET = "resgen"
RESGEN_AUTH0_CLIENT_ID = "NT4NPUbrBKU3N9HVcqLP8819P7ZD91iU"
RESGEN_AUTH0_DOMAIN = "https://auth.resgen.io"
# RESGEN_HOST = "http://localhost:8000"
# RESGEN_BUCKET = "resgen-test"

# ridiculously large number used to effectively turn
# off paging in requests
MAX_LIMIT = int(1e6)


def parse_ucsc(hub_string):
    # print("hub_string:", hub_string)

    things = []
    for hub_section in re.split("\n\n+", hub_string.strip()):
        parts = [d.split(maxsplit=1) for d in hub_section.split("\n") if d[0] != "#"]
        if parts:
            things += [dict(parts)]

    return things


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


class GeneAnnotation:
    def __init__(self, gene_info):
        self.name = gene_info["geneName"]
        self.tx_start = gene_info["txStart"]
        self.tx_end = gene_info["txEnd"]
        self.chrom = gene_info["chr"]


class ChromosomeInfo:
    def __init__(self):
        self.total_length = 0
        self.cum_chrom_lengths = {}
        self.chrom_lengths = {}
        self.chrom_order = []

    def to_abs(self, chrom: str, pos: int) -> int:
        """Calculate absolute coordinates."""
        return self.cum_chrom_lengths[chrom] + pos

    def to_abs_range(
        self,
        chrom: str,
        start: int,
        end: int,
        padding: float = 0,
        padding_abs: float = 0,
    ) -> typing.Tuple[int, int]:
        """Return a range along a chromosome with optional padding.
        """
        padding_abs += (end - start) * padding

        return [
            self.cum_chrom_lengths[chrom] + start - padding_abs,
            self.cum_chrom_lengths[chrom] + end + padding_abs,
        ]

    def to_gene_range(self, gene, padding: float = 0) -> typing.Tuple[int, int]:
        return self.to_abs_range(gene.chrom, gene.tx_start, gene.tx_end, padding)


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
        self.datafile = data["datafile"]
        self.uuid = data["uuid"]
        self.tags = []
        if "tags" in data:
            self.tags = data["tags"]

        self.name = None
        if "name" in data:
            self.name = data["name"]

    def __str__(self):
        """String representation."""
        return f"{self.uuid[:8]}: {self.name}"

    def __repr__(self):
        """String representation."""
        return f"{self.uuid[:8]}: {self.name}"

    def update(self, **kwargs):
        """Update this datasets metadata."""
        return self.conn.update_dataset(self.uuid, kwargs)

    def download_link(self):
        """Get a download link for this dataset."""
        ret = self.conn.authenticated_request(
            requests.get, f"{self.conn.host}/download/?d={self.uuid}"
        )

        if ret.status_code != 200:
            return UnknownConnectionException("Failed to get download link", ret)

        return ret.json()

    def hg_track(
        self, track_type=None, position=None, height=None, width=None, **options
    ):
        """Create a higlass track from this dataset."""
        datatype = tags_to_datatype(self.tags)

        if track_type is None:
            track_type, suggested_position = datatype_to_tracktype(datatype)

            if not position:
                position = suggested_position

        else:
            if position is None:
                position = tracktype_default_position(track_type)
            if position is None:
                raise ValueError(
                    f"No default position for track type: {track_type}. "
                    "Please specify a position"
                )

        return (track(
            track_type,
            height=height,
            width=width,
            tilesetUid=self.uuid,
            server=f"{self.conn.host}/api/v1",
            options=options,
        ), position)


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
            *args, **{**kwargs, "headers": {"Authorization": "Bearer {}".format(token)}}
        )

    def get_token(self) -> str:
        """Get a JWT token for interacting with the service."""
        if self.token:
            ret = requests.get(
                f"{self.host}/api/v1/which_user/",
                headers={"Authorization": "Bearer {}".format(self.token)},
            )

            if ret.status_code == 200:
                return self.token

        ret = requests.post(
            f"{RESGEN_AUTH0_DOMAIN}/oauth/token/",
            data={
                "username": self.username,
                "password": self.password,
                "grant_type": "password",
                "client_id": RESGEN_AUTH0_CLIENT_ID,
            },
        )

        if ret.status_code == 400:
            raise InvalidCredentialsException(
                "The provided username and password are incorrect"
            )
        elif ret.status_code != 200:
            raise UnknownConnectionException("Failed to login", ret)

        data = json.loads(ret.content.decode("utf8"))
        self.token = data["access_token"]
        return self.token

    def find_project(self, project_name: str, gruser: str = None):
        """Find a project."""
        name = gruser or self.username
        url = f"{self.host}/api/v1/projects/?n={name}&pn={project_name}&is=true"
        ret = self.authenticated_request(
            requests.get, url
        )

        if ret.status_code != 200:
            return UnknownConnectionException("Failed to fetch projects", ret)

        content = json.loads(ret.content)

        if content["count"] == 0:
            raise Exception("Project not found")

        if content["count"] > 1:
            raise Exception("More than one project found:", json.dumps(content))

        return ResgenProject(content["results"][0]["uuid"], self)

    def find_or_create_project(
        self, project_name: str, group: str = None, private: bool = True
    ):
        """Find or create a project.

        For now this function can only create a project for the
        logged in user. If a project with the same name exists, do
        nothing.

        Args:
            project_name: The name of the project to create.
            private: Whether to make this a private project.
        """

        url = f"{self.host}/api/v1/projects/?pn={project_name}"

        if group:
            url += f"&n={group}"
        ret = self.authenticated_request(requests.get, url)

        if ret.status_code == 404 or ret.json()["count"] == 0:
            url = f"{self.host}/api/v1/projects/"
            data = {"name": project_name, "private": private, "tilesets": []}
            if group:
                data = {**data, "gruser": group}

            ret = self.authenticated_request(requests.post, url, json=data)
            if ret.status_code == 409 or ret.status_code == 201:
                return ResgenProject(ret.json()["uuid"], self)
            raise UnknownConnectionException("Failed to create project", ret)

        return ResgenProject(ret.json()["results"][0]["uuid"], self)

    def list_projects(self, gruser: str = None):
        """List the projects of the connected user or the specified group.

        Args:
            gruser: The name of the user or group to list projects for.
                Defaults to the connected user if not specified.
        """
        gruser = gruser if gruser is not None else self.username

        # don't paginate because user's shouldn't have obscene numbers of
        # projects
        url = f"{self.host}/api/v1/projects/?n={gruser}&limit={MAX_LIMIT}"
        ret = self.authenticated_request(requests.get, url)

        if ret.status_code != 200:
            return UnknownConnectionException("Failed to retrieve projects", ret)

        retj = json.loads(ret.content)
        return [
            ResgenProject(proj["uuid"], self, proj["name"]) for proj in retj["results"]
        ]

    def get_dataset(self, uuid):
        """Retrieve a dataset."""
        url = f"{self.host}/api/v1/tilesets/{uuid}/"

        ret = self.authenticated_request(requests.get, url)

        if ret.status_code != 200:
            raise UnknownConnectionException("Unable to get dataset", ret)

        return ResgenDataset(self, json.loads(ret.content))

    def find_datasets(
        self, search_string="", project=None, limit=1000, datafile=None, **kwargs
    ):
        """Search for datasets."""
        tags_line = "&".join(
            [
                f"t={k}:{v}"
                for k, v in kwargs.items()
                if k not in ["search_string", "project", "limit"]
            ]
        )

        url = f"{self.host}/api/v1/list_tilesets/?limit={limit}&{tags_line}"

        if project:
            url += f"&ui={project.uuid}"
        if datafile:
            url += f"&df={datafile}"

        url += f"&ac={search_string}&limit={limit}"
        ret = self.authenticated_request(requests.get, url)

        if ret.status_code == 200:
            content = json.loads(ret.content)

            if content["count"] > limit:
                raise ValueError(
                    f"More datasets available ({content['count']}) than returned ({limit}))"
                )

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

    def get_gene(self, annotations_ds, gene_name):
        suggestions = self.get_genes(annotations_ds, gene_name)
        genes = [g for g in suggestions if g["geneName"].lower() == gene_name.lower()]

        if not genes:
            raise Exception(
                f"No such gene found: {gene_name}. Suggested: {str(suggestions)}"
            )
        if len(genes) > 1:
            raise Exception(f"More than one matching gene found: {str(genes)}")

        return GeneAnnotation(genes[0])

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
            url = f"{self.host}/api/v1/tilesets/{tileset_uuid}/"
            ret = self.authenticated_request(requests.get, url)

            if ret.status_code != 200:
                logger.error("Download failed")
                raise Exception(
                    "Failed to download dataset, "
                    + "make sure it exists at the given URL"
                )
            else:
                raise UnknownConnectionException(
                    "Failed to retrieve download progress", ret
                )

        return json.loads(ret.content)

    def upload_to_resgen_aws(
        self, filepath: str, prefix: str = None, index_filepath=None
    ) -> str:
        """
        Upload file to a resgen aws bucket.

        Args:
            filepath: The local filepath
            prefix: A prefix to upload to on the S3 bucket

        Returns:
            The path within the bucket where the object is uploaded

        """
        logger.info("Getting upload credentials for file: %s", filepath)
        url = f"{self.host}/api/v1/prepare_file_upload/"
        if prefix:
            url = f"{url}/?d={prefix}"
        ret = self.authenticated_request(requests.get, url)

        if ret.status_code != 200:
            raise UnknownConnectionException("Failed to prepare file upload", ret)

        content = json.loads(ret.content)
        filename = op.split(filepath)[1]

        directory_path = f"{content['fileDirectory']}/{filename}"
        object_name = f"{content['uploadBucketPrefix']}/{filename}"

        logger.info("Uploading to aws object: %s", object_name)
        index_directory_path = None
        if index_filepath:
            index_filename = op.split(index_filepath)[1]
            index_object_name = f"{content['uploadBucketPrefix']}/{index_filename}"
            index_directory_path = f"{content['fileDirectory']}/{index_filename}"
            logger.info("Uploading to aws index object: %s", index_object_name)

        bucket = self.bucket
        if aws.upload_file(filepath, bucket, content, object_name):
            if index_filepath:
                if not aws.upload_file(
                    index_filepath, bucket, content, index_object_name
                ):
                    return None
            return (directory_path, index_directory_path)

        return (None, None)

    def update_dataset(
        self, uuid: str, metadata: typing.Dict[str, typing.Any]
    ) -> ResgenDataset:
        """Update the properties of a dataset."""
        new_metadata = {}
        updatable_properties = ["name", "datafile", "tags", "description"]

        for key in metadata:
            if key not in updatable_properties:
                raise Exception(
                    f"Received property that can not be udpated: {key} "
                    f"Updatable properties: {str(updatable_properties)}"
                )
        if "name" in metadata:
            new_metadata["name"] = metadata["name"]
        if "description" in metadata:
            new_metadata["description"] = metadata["description"]
        if "datafile" in metadata:
            new_metadata["datafile"] = metadata["datafile"]
        if "tags" in metadata:
            new_metadata["tags"] = metadata["tags"]

        ret = self.authenticated_request(
            requests.patch, f"{self.host}/api/v1/tilesets/{uuid}/", json=new_metadata
        )

        if ret.status_code != 202:
            raise UnknownConnectionException("Failed to update dataset", ret)

        return self.get_dataset(uuid)


class ResgenProject:
    """Encapsulates a project on the resgen service."""

    def __init__(self, uuid: str, conn: ResgenConnection, name: str = None):
        """Initialize the project object.

        Args:
            uuid: The project's uuid on the resgen server
            conn: The resgen connection that this project was created with
            name: The name of the project. Not strictly necessary, but helpful
                when stringifying this object.
            try: Dry run... don't actually sync datasets

        """
        self.uuid = uuid
        self.conn = conn
        self.name = name

    def list_datasets(self, limit: int = 1000):
        """List the datasets available in this project.

        Returns up to a limit
        """
        return self.conn.find_datasets(project=self)

        # raise NotImplementedError()

    def add_link_dataset(self, filepath: str, index_filepath: str = None):
        """Add a remote dataset

        Args:
            filepath: The filename of the dataset to add. Can also be a url.
            index_filepath: The filename of the index for this dataset

        Returns:
            The uuid of the newly created dataset.

        """
        logger.info("Adding link dataset: %s", filepath)
        body = {
            "datafile": filepath,
            "private": True,
            "project": self.uuid,
            "download": False,
            "tags": [],
        }

        if index_filepath:
            body["indexfile"] = index_filepath

        ret = self.conn.authenticated_request(
            requests.post, f"{self.conn.host}/api/v1/tilesets/", json=body,
        )
        content = json.loads(ret.content)
        return content["uuid"]

    def add_download_dataset(self, filepath: str, index_filepath: str = None):
        """Add a dataset by downloading it from a remote source

        Args:
            filepath: The filename of the dataset to add. Can also be a url.
            index_filepath: The filename of the index for this dataset

        Returns:
            The uuid of the newly created dataset.

        """
        logger.info("Adding download dataset: %s", filepath)
        body = {
            "datafile": filepath,
            "private": True,
            "project": self.uuid,
            "description": f"Downloaded from {filepath}",
            "download": True,
            "tags": [],
        }

        if index_filepath:
            body["indexfile"] = index_filepath

        ret = self.conn.authenticated_request(
            requests.post, f"{self.conn.host}/api/v1/tilesets/", json=body,
        )

        if ret.status_code != 201:
            logger.error(
                "Error adding download dataset. Error code: %d, Content: %s",
                ret.status_code,
                ret.content,
            )
            return

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
                transferred = progress["downloaded"] + progress["uploaded"]
                to_transfer = 2 * progress["filesize"]
                percent_done = 100 * transferred / to_transfer

                sys.stdout.write(
                    f"\r {percent_done:.3f}% Complete ({transferred} of {to_transfer})"
                )

        return content["uuid"]

    def add_upload_dataset(self, filepath: str, index_filepath: str = None):
        """Add a dataset by uploading it to resgen

        Args:
            filepath: The filename of the dataset to add. Can also be a url.
            index_filepath: The filename of the index for this dataset

        Returns:
            The uuid of the newly created dataset.

        """
        logger.info("Adding upload dataset: %s", filepath)

        (directory_path, index_directory_path) = self.conn.upload_to_resgen_aws(
            filepath, index_filepath=index_filepath
        )

        if not directory_path:
            raise Exception("Error uploading to AWS")

        logger.info("Adding tileset entry for uploaded file: %s", directory_path)

        body = {
            "filepath": directory_path,
            "project": self.uuid,
        }

        if index_directory_path:
            body["indexpath"] = index_directory_path

        ret = self.conn.authenticated_request(
            requests.post, f"{self.conn.host}/api/v1/finish_file_upload/", json=body,
        )

        if ret.status_code != 200:
            raise UnknownConnectionException("Failed to finish uploading file", ret)

        content = json.loads(ret.content)
        return content["uuid"]

    def add_dataset(
        self,
        filepath: str,
        download: bool = False,
        index_filepath: str = None,
        sync_remote: bool = False,
    ):
        if download:
            if sync_remote:
                return self.add_download_dataset(filepath, index_filepath)
            else:
                return self.add_link_dataset(filepath, index_filepath)
        else:
            return self.add_upload_dataset(filepath, index_filepath)

    def delete_dataset(self, uuid: str):
        """Delete a dataset."""
        ret = self.conn.authenticated_request(
            requests.delete, f"{self.conn.host}/api/v1/tilesets/{uuid}/"
        )

        if ret.status_code != 204:
            raise UnknownConnectionException("Failed to delete dataset", ret)

        return uuid

    def sync_viewconf(self, viewconf, name):
        """Create a viewconf if it doesn't exist and update it if it does."""
        # try to get a viewconf with that name
        ret = self.conn.authenticated_request(
            requests.get, f"{self.conn.host}/api/v1/list_viewconfs/?n={name}"
        )

        if ret.status_code != 200:
            raise UnknownConnectionException("Failed to retrieve viewconfs", ret)

        content = json.loads(ret.content)

        if content["count"] > 1:
            raise ValueError(
                "More than one viewconf with that name:", json.dumps(content, indent=2)
            )

        if content["count"] == 0:
            return self.add_viewconf(viewconf, name)
        else:
            self.add_viewconf(viewconf, name)
            self.delete_viewconf(content["results"][0]["uuid"])

    def delete_viewconf(self, uuid):
        """Delete a viewconf."""
        ret = self.conn.authenticated_request(
            requests.delete, f"{self.conn.host}/api/v1/viewconfs/{uuid}/"
        )

        if ret.status_code != 204:
            raise UnknownConnectionException("Unable to delete viewconf:", ret)

    def add_viewconf(self, viewconf, name):
        """Save a viewconf to this project."""
        if isinstance(viewconf, Viewconf):
            viewconf = viewconf.dict()

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

        if ret.status_code != 201:
            raise UnknownConnectionException("Unable to add viewconf", ret)

    def __str__(self):
        """String representation."""
        return self.__repr__

    def __repr__(self):
        """String representation."""
        return f"{self.uuid[:8]}: {self.name}"

    def sync_track_hub(self, base_url, dry=False):
        """Sync a UCSC track hub."""
        hub_url = f"{base_url}/hub.txt"

        ret = requests.get(hub_url)

        content = ret.content.decode("utf8")
        hub_info = parse_ucsc(content)[0]

        genomes_url = f'{base_url}/{hub_info["genomesFile"]}'
        ret = requests.get(genomes_url)

        content = ret.content.decode("utf8")
        genome_infos = parse_ucsc(content)

        for genome_info in genome_infos:
            self.sync_genome(base_url, genome_info, dry)

    def sync_genome(self, base_url, genome_info, dry=False):
        """Sync a genome within a track hub."""
        track_db_url = f"{base_url}/{genome_info['trackDb']}"

        ret = requests.get(track_db_url)
        content = ret.content.decode("utf8")
        genome_info_path = op.split(genome_info["trackDb"])[0]

        track_infos = parse_ucsc(content)
        for track in track_infos:
            type_parts = track["type"].split()
            track_type = type_parts[0]

            if track_type in ["bigWig", "bigBed"] and track.get("bigDataUrl"):
                big_data_path = (
                    f"{base_url}/{genome_info_path}/{track.get('bigDataUrl')}"
                )

                if track_type == "bigWig":
                    datatype = "vector"
                elif track_type == "bigBed" and type_parts[1] == 12:
                    datatype = "gene-annotations"
                else:
                    datatype = "bedlike"

                assembly = genome_info["genome"]
                name = track.get("shortLabel")
                description = track.get("longLabel")

                if not dry:
                    self.sync_dataset(
                        big_data_path,
                        sync_remote=False,
                        datatype=datatype,
                        filetype=track_type.lower(),
                        assembly=assembly,
                        name=name,
                        description=description,
                    )
                else:
                    print(f"Syncing: {big_data_path} assembly: {assembly}")

    def sync_dataset(
        self,
        filepath: str,
        sync_remote: bool = False,
        filetype=None,
        datatype=None,
        assembly=None,
        index_filepath=None,
        force_update: bool = False,
        sync_full_path: bool = False,
        **metadata,
    ):
        """Check if this file already exists in this dataset.

        Do nothing if it does and create it if it doesn't. If a new
        dataset is created.

        In both instances, ensure that the metadata is updated. The available
        metadata tags that can be updated are: `name` and `tags`

        If more than one dataset with this name exists, raise a ValueError.

        Args:

        """
        logger.info("Syncing dataset: %s", filepath)
        if (
            filepath.startswith("http://")
            or filepath.startswith("https://")
            or filepath.startswith("ftp://")
        ):
            download = True
        else:
            download = False

        if not sync_full_path:
            filename = op.split(filepath)[1]
        else:
            filename = filepath

        logger.info("Filename used to sync: %s", filename)

        try:
            datasets = self.conn.find_datasets(project=self, datafile=filename)
        except UnknownConnectionException:
            logger.info("No such datasets found")
            datasets = []

        # filetype, datatype = fill_filetype_and_datatype(filename, filetype, datatype)

        def ds_filename(dataset):
            """Return just the filename of a dataset."""
            if not sync_full_path:
                filename = op.split(dataset.data["datafile"])[1]
            else:
                return dataset.data["datafile"]
            return filename

        matching_datasets = [d for d in datasets if ds_filename(d) == filename]

        if len(matching_datasets) > 1:
            raise ValueError(
                f"More than one matching dataset: {str(matching_datasets)}"
            )

        if not matching_datasets:
            uuid = self.add_dataset(
                filepath,
                download=download,
                index_filepath=index_filepath,
                sync_remote=sync_remote,
            )
        else:
            logger.info("Found dataset with the same filepath, updating metadata")
            uuid = matching_datasets[0].data["uuid"]

            if force_update:
                new_uuid = self.add_dataset(
                    filepath,
                    download=download,
                    index_filepath=index_filepath,
                    sync_remote=sync_remote,
                )
                self.delete_dataset(uuid)
                uuid = new_uuid

        if not filetype:
            filetype = infer_filetype(filepath)
            logger.info(f"Inferred filetype: {filetype}")
        if not datatype:
            datatype = infer_datatype(filetype)
            logger.info(f"Inferred datatype: {datatype}")

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

        return self.conn.update_dataset(uuid, to_update)


def connect(
    username: str = None,
    password: str = None,
    host: str = RESGEN_HOST,
    bucket: str = RESGEN_BUCKET,
) -> ResgenConnection:
    """Open a connection to resgen."""
    env_path = Path.home() / ".resgen" / "credentials"

    if env_path.exists():
        load_dotenv(env_path)

        username = os.getenv("RESGEN_USERNAME")
        password = os.getenv("RESGEN_PASSWORD")
    else:
        username = input("Username:")
        password = getpass.getpass("Password:")

    if username is None:
        username = os.getenv("RESGEN_USERNAME")
    if password is None:
        password = os.getenv("RESGEN_PASSWORD")

    return ResgenConnection(username, password, host, bucket)
