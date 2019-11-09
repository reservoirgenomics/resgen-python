import json
import logging
import os.path as op
import requests

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

    pass


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

        login = requests.post(
            f"{self.host}/api-token-auth/",
            data={"username": self.username, "password": self.password},
        )

        if login.status_code == 400:
            raise InvalidCredentialsException(
                "The provided username and password are incorrect"
            )
        elif login.status_code != 200:
            raise UnknownConnectionException(
                "Failed to login, "
                f"status_code: {login.status_code}, "
                f"content: {login.content}"
            )

        data = json.loads(login.content.decode("utf8"))

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

        raise UnknownConnectionException(
            "Failed to create project: "
            f"status_code: {ret.status_code}, "
            f"content: {ret.content}"
        )


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
            raise UnknownConnectionException(
                "Failed to retrieve tilesets: "
                f"status_code: {ret.status_code}, "
                f"content: {ret.content}"
            )

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

        print("ret:", ret)
        print("ret.content", ret.content)

        content = json.loads(ret.content)
        filename = op.split(filepath)[1]
        directory_path = f"{content['fileDirectory']}/{filename}"
        object_name = f"{content['uploadBucketPrefix']}/{filename}"

        bucket = RESGEN_BUCKET
        print("Uploading object name %s to bucket %s", object_name, bucket)
        if aws.upload_file(filepath, bucket, content, object_name):
            ret = requests.post(
                f"{self.conn.host}/api/v1/finish_file_upload/",
                headers={"Authorization": "JWT {}".format(token)},
                json={"filepath": directory_path, "project": self.uuid},
            )

            content = json.loads(ret.content)
            return content["uuid"]

    def sync_dataset(self, filepath: str):
        """Check if this file already exists in this dataset.

        Do nothing if it does and create it if it doesn't.
        """
        datasets = self.list_datasets()
        filename = op.split(filepath)[1]

        print("filename:", filename)

        def ds_filename(dataset):
            """Return just the filename of a dataset."""
            fn = op.split(dataset["datafile"][1])
            print("fn:", fn)

        matching_datasets = [d for d in datasets if ds_filename(d) == filename]

        print("datasets:", datasets)
        print("matching_datasets", matching_datasets)

        pass


def connect(username: str, password: str, host: str = RESGEN_HOST) -> ResgenConnection:
    """Obtain a connection to resgen."""
    return ResgenConnection(username, password, host)
