import json
import os.path as op
import requests
import tempfile

from unittest.mock import patch, MagicMock

import resgen

# class MockConnection:
#     def __init__(self):
#         pass

#     def get_token(self):
#         return 'xx'

# class MockProject:
#     def __init__(self):
#         self.conn = MockConnection()


def test_sync_dataset_new():
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = json.dumps(
            {"results": [{"datafile": "aws/TbUN0fR-RDW_Ob2wk5KRkg/blah1.txt"}]}
        )

        filepath = op.join(tempfile.mkdtemp(), "/tmp/blah.txt")
        with open(filepath, "w") as f:
            f.write("hello")

        project = resgen.ResgenProject("xxx", MagicMock())
        project.add_dataset = MagicMock()

        project.sync_dataset(filepath)
        assert project.add_dataset.called

        mock_get.return_value.content = json.dumps(
            {"results": [{"datafile": "aws/TbUN0fR-RDW_Ob2wk5KRkg/blah.txt"}]}
        )

        # if a file with the same name exists, we shouldn't try
        # adding a new one
        project.add_dataset = MagicMock()
        project.sync_dataset(filepath)
        assert not project.add_dataset.called
