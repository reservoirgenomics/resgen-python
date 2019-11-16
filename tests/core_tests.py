from contextlib import ExitStack
import json
import os.path as op
import requests
import tempfile

from unittest.mock import patch, MagicMock

import resgen as rg


def test_create_tags():
    tags = rg.update_tags({}, datatype="matrix")

    assert tags[0]["name"] == "datatype:matrix"


def test_sync_dataset_new():
    filepath = op.join(tempfile.mkdtemp(), "/tmp/blah.txt")
    with open(filepath, "w") as f:
        f.write("hello")

    project = rg.ResgenProject("xxx", MagicMock())
    project.conn = MagicMock()
    project.conn.authenticated_request.return_value.status_code = 200
    project.conn.authenticated_request.return_value.content = json.dumps(
        {"results": []}
    )

    project.update_dataset = MagicMock()
    project.add_dataset = MagicMock()

    project.sync_dataset(filepath)
    assert project.add_dataset.called

    project.list_datasets = MagicMock()
    project.list_datasets.return_value = [
        {"uuid": "xx", "datafile": "aws/TbUN0fR-RDW_Ob2wk5KRkg/blah.txt"}
    ]

    # if a file with the same name exists, we shouldn't try
    # adding a new one
    project.add_dataset = MagicMock()
    project.sync_dataset(filepath)
    assert not project.add_dataset.called
