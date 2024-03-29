import json
import os.path as op
import tempfile
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import requests
import requests_mock

import resgen as rg


def test_create_tags():
    tags = rg.update_tags({}, datatype="matrix")

    assert tags[0]["name"] == "datatype:matrix"


def test_list_projects():
    with requests_mock.Mocker() as m:
        m.get(
            f"{rg.RESGEN_HOST}/api/v1/projects/?n=user&limit=1000000",
            json={"results": [{"uuid": "u1", "name": "blah"}]},
        )
        m.get(f"{rg.RESGEN_HOST}/api/v1/which_user/", json={"not": "important"})
        m.post(f"{rg.RESGEN_AUTH0_DOMAIN}/oauth/token/", json={"access_token": "xy"})

        rgc = rg.ResgenConnection("user", "password")
        projects = rgc.list_projects()

        assert len(projects) == 1
        assert projects[0].name == "blah"


def test_sync_dataset_new():
    filepath = op.join(tempfile.mkdtemp(), "blah.txt")
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

    project.conn.find_datasets = MagicMock()
    project.conn.find_datasets.return_value = [
        rg.ResgenDataset(
            conn=project.conn,
            data={"uuid": "xx", "datafile": "aws/TbUN0fR-RDW_Ob2wk5KRkg/blah.txt"},
        )
    ]

    # if a file with the same name exists, we shouldn't try
    # adding a new one
    project.add_dataset = MagicMock()
    project.sync_dataset(filepath)
    assert not project.add_dataset.called
