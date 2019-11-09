import json
import requests
import tempfile

from unittest.mock import patch

import resgen


def test_sync_dataset():
    with patch("requests.get") as mock_get:
        mock_get.return_value.content = json.dumps(
            {
                "count": 1,
                "next": None,
                "previous": None,
                "results": [
                    {
                        "uuid": "Mi5Act1eQjqaai6Ib2Wasg",
                        "datafile": "aws/TbUN0fR-RDW_Ob2wk5KRkg/blah.txt",
                        "indexfile": None,
                        "filetype": "",
                        "filesize": 5,
                        "datatype": "unknown",
                        "private": False,
                        "name": "blah.txt",
                        "coordSystem": "",
                        "coordSystem2": "",
                        "created": "2019-11-09T21:05:05.917755Z",
                        "owner": "test",
                        "tags": [],
                        "project_name": "test-project",
                        "project_owner": "test",
                        "description": "",
                    }
                ],
            }
        )

        filepath = op.join(tempfile.mkdtemp(), "/tmp/blah.txt")
        with open(filepath, "w") as f:
            f.write("hello")

        project = resgen.ResgenProject("xxx")
        project.sync_dataset(filepath)
