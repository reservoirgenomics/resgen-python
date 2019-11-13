import json
import logging
import os.path as op
import requests
import typing

from resgen import aws

logger = logging.getLogger(__name__)

__version__ = "0.1.0"

RESGEN_HOST = "https://resgen.io"
RESGEN_BUCKET = "resgen"

# RESGEN_HOST = "http://localhost:8000"
# RESGEN_BUCKET = "resgen-test"


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


class ResgenConnection:
    """Connection to the resgen server."""

    def __init__(self, username, password, host=RESGEN_HOST):
        self.username = username
        self.password = password
        self.host = host

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

    def create_project(self, project_name: str, private: bool = True):
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

    def add_dataset(self, filepath: str):
        """Add a dataset

        Args:
            filepath: The filename of the dataset to add.

        Returns:
            The uuid of the newly created dataset.

        """
        ret = self.conn.authenticated_request(
            requests.get, f"{self.conn.host}/api/v1/prepare_file_upload/"
        )

        if ret.status_code != 200:
            raise UnknownConnectionException("Failed to prepare file upload", ret)

        content = json.loads(ret.content)
        filename = op.split(filepath)[1]
        directory_path = f"{content['fileDirectory']}/{filename}"
        object_name = f"{content['uploadBucketPrefix']}/{filename}"

        bucket = RESGEN_BUCKET
        if aws.upload_file(filepath, bucket, content, object_name):
            ret = self.conn.authenticated_request(
                requests.post,
                f"{self.conn.host}/api/v1/finish_file_upload/",
                json={"filepath": directory_path, "project": self.uuid},
            )

            if ret.status_code != 200:
                raise UnknownConnectionException("Failed to finish uploading file", ret)

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
        """
        datasets = self.list_datasets()
        filename = op.split(filepath)[1]

        def ds_filename(dataset):
            """Return just the filename of a dataset."""
            fn = op.split(dataset["datafile"])[1]
            return fn

        matching_datasets = [d for d in datasets if ds_filename(d) == filename]

        if len(matching_datasets) > 1:
            raise ValueError("More than one matching dataset")

        if not len(matching_datasets):
            uuid = self.add_dataset(filepath)
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

        self.update_dataset(uuid, to_update)


def connect(username: str, password: str, host: str = RESGEN_HOST) -> ResgenConnection:
    """Obtain a connection to resgen."""
    return ResgenConnection(username, password, host)
