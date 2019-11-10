import json
import logging
import os.path as op
import requests
import typing

from resgen import aws

logger = logging.getLogger(__name__)

__version__ = "0.1.0"

# RESGEN_API = "https://resgen.io/api/v1"
# RESGEN_BUCKET = "resgen"

RESGEN_HOST = "http://localhost:8000"
RESGEN_API = f"{RESGEN_HOST}/api/v1"
RESGEN_BUCKET = "resgen-test"


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

    def get_token(self) -> str:
        """Get a JWT token for interacting with the service."""
        if self.token:
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

        return data["token"]

    def create_project(self, project_name: str, private: bool = True):
        """Create a project.

        For now this function can only create a project for the
        logged in user. If a project with the same name exists, do
        nothing.

        Args:
            project_name: The name of the project to create.
            private: Whether to make this a private project.
        """
        token = self.get_token()

        ret = requests.post(
            f"{self.host}/api/v1/projects/",
            json={"name": project_name, "private": private, "tilesets": []},
            headers={"Authorization": "JWT {}".format(token)},
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
        token = self.conn.get_token()
        url = f"{self.conn.host}/api/v1/list_tilesets/?ui={self.uuid}&offset=0"
        ret = requests.get(url, headers={"Authorization": "JWT {}".format(token)})

        if ret.status_code != 200:
            raise UnknownConnectionException("Failed to retrieve tilesets", ret)

        print("ret.content:", ret.content)
        return json.loads(ret.content)["results"]

        # raise NotImplementedError()

    def add_dataset(self, filepath: str):
        """Add a dataset

        Args:
            filepath: The filename of the dataset to add.

        Returns:
            The uuid of the newly created dataset.

        """
        token = self.conn.get_token()

        ret = requests.get(
            f"{self.conn.host}/api/v1/prepare_file_upload/",
            headers={"Authorization": "JWT {}".format(token)},
        )

        content = json.loads(ret.content)
        filename = op.split(filepath)[1]
        directory_path = f"{content['fileDirectory']}/{filename}"
        object_name = f"{content['uploadBucketPrefix']}/{filename}"

        bucket = RESGEN_BUCKET
        if aws.upload_file(filepath, bucket, content, object_name):
            ret = requests.post(
                f"{self.conn.host}/api/v1/finish_file_upload/",
                headers={"Authorization": "JWT {}".format(token)},
                json={"filepath": directory_path, "project": self.uuid},
            )

            content = json.loads(ret.content)
            return content["uuid"]

    def update_dataset(self, uuid: str, metadata: typing.Dict[str, typing.Any]):
        """Update the properties of a dataset."""
        token = self.conn.get_token()

        new_metadata = {}

        if "name" in metadata:
            new_metadata["name"] = metadata["name"]
        if "tags" in metadata:
            new_metadata["tags"] = metadata["tags"]

        ret = requests.patch(
            f"{self.conn.host}/api/v1/tilesets/{uuid}/",
            headers={"Authorization": "JWT {}".format(token)},
            json=new_metadata,
        )

        if ret.status_code != 202:
            raise UnknownConnectionException("Failed to update dataset", ret)

        return uuid

    def sync_dataset(self, filepath: str, metadata: typing.Dict[str, typing.Any]):
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

        to_update = {}
        if "name" in metadata:
            to_update["name"] = metadata["name"]
        if "tags" in metadata:
            to_update["tags"] = metadata["tags"]

        self.update_dataset(uuid, to_update)


def connect(username: str, password: str, host: str = RESGEN_HOST) -> ResgenConnection:
    """Obtain a connection to resgen."""
    return ResgenConnection(username, password, host)
