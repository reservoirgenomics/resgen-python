import logging
import os
import sys
import threading
import typing

import boto3
from botocore.exceptions import ClientError


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)"
                % (self._filename, self._seen_so_far, self._size, percentage)
            )
            sys.stdout.flush()


def upload_file(
    file_name: str,
    bucket: str,
    credentials: typing.Dict[str, str],
    object_name: str = None,
):
    """Upload a file to an S3 bucket

    Args:
        file_name: File to upload
        bucket: Bucket to upload to
        credentials: A dictionary containing the `awsAccessKeyId`,
            `secretAccessKey` and `sessionToken` entries
        object_name: S3 object name. If not specified then file_name is used

    Returns:
        True if file was uploaded, else False

    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=credentials["accessKeyId"],
        aws_secret_access_key=credentials["secretAccessKey"],
        aws_session_token=credentials["sessionToken"],
    )

    try:
        s3_client.upload_file(
            file_name, bucket, object_name, Callback=ProgressPercentage(file_name)
        )
    except ClientError as client_error:
        logging.error(client_error)
        return False
    return True
