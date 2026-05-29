import os
import tempfile
from unittest.mock import MagicMock, patch, call
import pytest

from resgen.manage import _sync_datasets, can_sync_datasets, get_container_runtime
from resgen.license import LicenseInfo, LicenseError
from resgen.exceptions import ResgenError
from resgen import UnknownConnectionException
from resgen.sync.folder import get_s3_datasets, load_s3_mounts, save_s3_mounts


class TestGetContainerRuntime:
    """Test suite for the get_container_runtime function."""

    def test_env_var_takes_precedence(self):
        """Test that RESGEN_CONTAINER_RUNTIME env var overrides auto-detection."""
        with patch.dict(os.environ, {"RESGEN_CONTAINER_RUNTIME": "finch"}):
            with patch("shutil.which", return_value="/usr/local/bin/docker"):
                assert get_container_runtime() == "finch"

    def test_env_var_docker(self):
        """Test that RESGEN_CONTAINER_RUNTIME can explicitly select docker."""
        with patch.dict(os.environ, {"RESGEN_CONTAINER_RUNTIME": "docker"}):
            with patch("shutil.which", return_value=None):
                assert get_container_runtime() == "docker"

    def test_autodetect_docker_when_only_docker_present(self):
        """Test that docker is selected when only docker is in PATH."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("RESGEN_CONTAINER_RUNTIME", None)
            with patch("shutil.which", side_effect=lambda x: "/usr/bin/docker" if x == "docker" else None):
                assert get_container_runtime() == "docker"

    def test_autodetect_finch_when_only_finch_present(self):
        """Test that finch is selected when only finch is in PATH."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("RESGEN_CONTAINER_RUNTIME", None)
            with patch("shutil.which", side_effect=lambda x: "/usr/local/bin/finch" if x == "finch" else None):
                assert get_container_runtime() == "finch"

    def test_autodetect_prefers_docker_when_both_present(self):
        """Test that docker is preferred over finch when both are available."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("RESGEN_CONTAINER_RUNTIME", None)
            with patch("shutil.which", return_value="/usr/local/bin/runtime"):
                assert get_container_runtime() == "docker"

    def test_fallback_to_docker_when_neither_present(self):
        """Test that docker is returned as fallback when neither runtime is in PATH."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("RESGEN_CONTAINER_RUNTIME", None)
            with patch("shutil.which", return_value=None):
                assert get_container_runtime() == "docker"


class TestManageCommandsUseRuntime:
    """Test that manage commands delegate to the configured container runtime."""

    @patch("resgen.manage.get_container_runtime", return_value="finch")
    @patch("resgen.manage.run")
    def test_stop_uses_runtime(self, mock_run, mock_runtime):
        """Test that stop passes the detected runtime to compose down."""
        from resgen.manage import stop
        from click.testing import CliRunner

        with tempfile.TemporaryDirectory() as tmpdir:
            compose_dir = os.path.join(tmpdir, ".resgen", "config")
            os.makedirs(compose_dir)
            compose_file = os.path.join(compose_dir, "stack.yml")
            with open(compose_file, "w") as f:
                f.write("")

            result = CliRunner().invoke(stop, [tmpdir])

        assert mock_runtime.called
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "finch"
        assert "compose" in cmd

    @patch("resgen.manage.get_container_runtime", return_value="finch")
    @patch("resgen.manage.run")
    def test_pull_uses_runtime(self, mock_run, mock_runtime):
        """Test that pull passes the detected runtime to the pull subcommand."""
        from resgen.manage import pull
        from click.testing import CliRunner

        CliRunner().invoke(pull, [])

        assert mock_runtime.called
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "finch"
        assert cmd[1] == "pull"

    @patch("resgen.manage.get_container_runtime", return_value="finch")
    @patch("resgen.manage.run")
    def test_update_uses_runtime(self, mock_run, mock_runtime):
        """Test that update passes the detected runtime to the pull subcommand."""
        from resgen.manage import update
        from click.testing import CliRunner

        CliRunner().invoke(update, [])

        assert mock_runtime.called
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "finch"
        assert cmd[1] == "pull"


class TestSyncDatasets:
    """Test suite for the sync_datasets command."""

    @patch("resgen.manage.rg")
    @patch("resgen.manage._get_directory_url")
    @patch("resgen.manage.get_local_datasets")
    @patch("resgen.manage.get_remote_datasets")
    @patch("resgen.manage.add_and_update_local_datasets")
    @patch("resgen.manage.remove_stale_remote_datasets")
    @patch("resgen.manage.can_sync_datasets")
    def test_sync_datasets_success(
        self,
        mock_can_sync,
        mock_remove_stale,
        mock_add_update,
        mock_get_remote,
        mock_get_local,
        mock_get_url,
        mock_rg,
    ):
        """Test successful sync of datasets."""
        # Setup
        directory = "/test/dir"
        mock_get_url.return_value = "http://localhost:1807"
        
        mock_connection = MagicMock()
        mock_project = MagicMock()
        mock_rg.connect.return_value = mock_connection
        mock_connection.find_or_create_project.return_value = mock_project
        
        local_datasets = [{"name": "test.txt", "fullpath": "test.txt"}]
        remote_datasets = {}
        mock_get_local.return_value = local_datasets
        mock_get_remote.return_value = remote_datasets

        # Execute
        _sync_datasets(directory)

        # Verify
        mock_rg.connect.assert_called_once_with(
            username="local", password="local", host="http://localhost:1807", auth_provider="local"
        )
        mock_connection.find_or_create_project.assert_called_once_with("dir")
        mock_get_local.assert_called_once_with(directory)
        mock_get_remote.assert_called_once_with(mock_project)
        mock_can_sync.assert_called_once_with(directory, 1)
        mock_add_update.assert_called_once_with(
            mock_project, local_datasets, remote_datasets, base_directory=directory, link=True
        )
        mock_remove_stale.assert_called_once_with(mock_project, local_datasets, remote_datasets)

    @patch("resgen.manage._get_directory_url")
    def test_sync_datasets_no_running_container(self, mock_get_url):
        """Test sync when no running container is found."""
        mock_get_url.return_value = None
        
        with patch("resgen.manage.logger") as mock_logger:
            _sync_datasets("/test/dir")
            mock_logger.error.assert_called_once_with(
                "No running resgen container found for directory: /test/dir"
            )

    @patch("resgen.manage.rg")
    @patch("resgen.manage._get_directory_url")
    @patch("resgen.manage.get_local_datasets")
    @patch("resgen.manage.can_sync_datasets")
    def test_sync_datasets_connection_error(self, mock_can_sync, mock_get_local, mock_get_url, mock_rg):
        """Test sync when connection fails."""
        mock_get_url.return_value = "http://localhost:1807"
        mock_get_local.return_value = []
        mock_rg.UnknownConnectionException = UnknownConnectionException
        mock_rg.connect.side_effect = UnknownConnectionException("Test error", MagicMock())
        
        with patch("resgen.manage.logger") as mock_logger:
            _sync_datasets("/test/dir")
            mock_logger.error.assert_called_once_with(
                "Unable to login, please check your username and password"
            )

    @patch("resgen.manage.rg")
    @patch("resgen.manage._get_directory_url")
    @patch("resgen.manage.get_local_datasets")
    @patch("resgen.manage.get_remote_datasets")
    @patch("resgen.manage.add_and_update_local_datasets")
    @patch("resgen.manage.can_sync_datasets")
    def test_sync_datasets_resgen_error(
        self,
        mock_can_sync,
        mock_add_update,
        mock_get_remote,
        mock_get_local,
        mock_get_url,
        mock_rg,
    ):
        """Test sync when ResgenError is raised."""
        # Setup
        mock_get_url.return_value = "http://localhost:1807"
        mock_connection = MagicMock()
        mock_project = MagicMock()
        mock_rg.connect.return_value = mock_connection
        mock_connection.find_or_create_project.return_value = mock_project
        
        mock_get_local.return_value = []
        mock_get_remote.return_value = {}
        mock_add_update.side_effect = ResgenError("Test error")

        with patch("resgen.manage.logger") as mock_logger:
            _sync_datasets("/test/dir")
            mock_logger.error.assert_called_once_with("Test error")

    @patch("resgen.manage.get_license")
    def test_can_sync_datasets_guest_within_limit(self, mock_get_license):
        """Test can_sync_datasets with guest license within limit."""
        mock_license = LicenseInfo(permissions="guest", username="guest")
        mock_get_license.return_value = mock_license
        
        with patch("resgen.manage.datasets_allowed", return_value=5):
            # Should not raise exception
            can_sync_datasets("/test/dir", 3)

    @patch("resgen.manage.get_license")
    def test_can_sync_datasets_guest_exceeds_limit(self, mock_get_license):
        """Test can_sync_datasets with guest license exceeding limit."""
        mock_license = LicenseInfo(permissions="guest", username="guest")
        mock_get_license.return_value = mock_license
        
        with patch("resgen.manage.datasets_allowed", return_value=2):
            with pytest.raises(LicenseError, match="Guest license has exceeded"):
                can_sync_datasets("/test/dir", 5)

    @patch("resgen.manage.get_license")
    def test_can_sync_datasets_non_guest(self, mock_get_license):
        """Test can_sync_datasets with non-guest license."""
        mock_license = LicenseInfo(permissions="admin", username="user")
        mock_get_license.return_value = mock_license
        
        # Should not raise exception regardless of dataset count
        can_sync_datasets("/test/dir", 1000)

    @patch("resgen.manage.rg")
    @patch("resgen.manage._get_directory_url")
    @patch("resgen.manage.get_remote_datasets")
    @patch("resgen.manage.remove_stale_remote_datasets")
    @patch("resgen.manage.can_sync_datasets")
    def test_sync_fna_with_index_creates_datasets(self, mock_can_sync, mock_remove_stale, mock_get_remote, mock_get_url, mock_rg):
        """Test that .fna and .fna.fai files create datasets with .fai as both index and separate dataset."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            fna_file = os.path.join(temp_dir, "test.fna")
            fai_file = os.path.join(temp_dir, "test.fna.fai")
            
            with open(fna_file, "w") as f:
                f.write(">seq1\nATCG\n")
            with open(fai_file, "w") as f:
                f.write("seq1\t4\t6\t4\t5\n")
            
            # Setup mocks
            mock_get_url.return_value = "http://localhost:1807"
            mock_connection = MagicMock()
            mock_project = MagicMock()
            mock_rg.connect.return_value = mock_connection
            mock_connection.find_or_create_project.return_value = mock_project
            mock_get_remote.return_value = {}
            
            # Mock the add_and_update_local_datasets to capture the datasets
            with patch("resgen.manage.add_and_update_local_datasets") as mock_add_update:
                _sync_datasets(temp_dir)
                
                # Verify add_and_update_local_datasets was called
                mock_add_update.assert_called_once()
                call_args = mock_add_update.call_args[0]
                local_datasets = call_args[1]
                
                # Should have two datasets: .fna with index and .fai as separate dataset
                assert len(local_datasets) == 2
                
                # Find the datasets
                fna_dataset = next(d for d in local_datasets if d["fullpath"] == "test.fna")
                fai_dataset = next(d for d in local_datasets if d["fullpath"] == "test.fna.fai")
                
                # The .fna dataset should have the .fai file as indexfile
                assert fna_dataset["index_filepath"] == "test.fna.fai"
                # The .fai dataset should exist as a separate dataset
                assert fai_dataset["name"] == "test.fna.fai"


class TestS3Mounts:
    """Test suite for S3 mount management."""

    def test_save_and_load_s3_mounts(self):
        """Test saving and loading S3 mount configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            mounts = [
                {"path": "s3://bucket1/data", "folder": "data1"},
                {"path": "s3://bucket2/ref", "folder": "refs"},
            ]

            save_s3_mounts(temp_dir, mounts)
            loaded_mounts = load_s3_mounts(temp_dir)

            assert loaded_mounts == mounts

    def test_load_s3_mounts_no_file(self):
        """Test loading S3 mounts when no config file exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            mounts = load_s3_mounts(temp_dir)
            assert mounts == []

    def test_save_s3_mounts_creates_directory(self):
        """Test that save_s3_mounts creates .resgen directory if needed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            mounts = [{"path": "s3://bucket/data", "folder": "data"}]
            save_s3_mounts(temp_dir, mounts)

            assert os.path.exists(os.path.join(temp_dir, ".resgen"))
            assert os.path.exists(os.path.join(temp_dir, ".resgen", "mounts.yml"))

    @patch("resgen.sync.folder.get_local_datasets")
    def test_s3_add_command_success(self, mock_get_local):
        """Test successful S3 mount addition."""
        from resgen.manage import s3_add
        from click.testing import CliRunner

        with tempfile.TemporaryDirectory() as temp_dir:
            mock_get_local.return_value = []

            runner = CliRunner()
            result = runner.invoke(
                s3_add,
                ["s3://test-bucket/data", temp_dir, "--folder", "test-data"]
            )

            assert result.exit_code == 0
            mounts = load_s3_mounts(temp_dir)
            assert len(mounts) == 1
            assert mounts[0]["path"] == "s3://test-bucket/data"
            assert mounts[0]["folder"] == "test-data"

    @patch("resgen.sync.folder.get_local_datasets")
    def test_s3_add_command_folder_conflict(self, mock_get_local):
        """Test S3 mount addition fails when folder exists locally."""
        from resgen.manage import s3_add
        from click.testing import CliRunner

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a local folder
            os.makedirs(os.path.join(temp_dir, "existing"))

            mock_get_local.return_value = [
                {"name": "existing", "fullpath": "existing", "is_folder": True}
            ]

            runner = CliRunner()
            result = runner.invoke(
                s3_add,
                ["s3://test-bucket/data", temp_dir, "--folder", "existing"]
            )

            # Should fail with error message
            assert result.exit_code == 0  # Click command succeeds but logs error
            mounts = load_s3_mounts(temp_dir)
            assert len(mounts) == 0  # Mount not added

    def test_s3_remove_command(self):
        """Test S3 mount removal."""
        from resgen.manage import s3_remove
        from click.testing import CliRunner

        with tempfile.TemporaryDirectory() as temp_dir:
            # Add a mount first
            mounts = [
                {"path": "s3://bucket1/data", "folder": "data1"},
                {"path": "s3://bucket2/ref", "folder": "refs"},
            ]
            save_s3_mounts(temp_dir, mounts)

            runner = CliRunner()
            result = runner.invoke(s3_remove, ["data1", temp_dir])

            assert result.exit_code == 0
            loaded_mounts = load_s3_mounts(temp_dir)
            assert len(loaded_mounts) == 1
            assert loaded_mounts[0]["folder"] == "refs"

    def test_s3_list_command(self):
        """Test S3 mount listing."""
        from resgen.manage import s3_list
        from click.testing import CliRunner

        with tempfile.TemporaryDirectory() as temp_dir:
            mounts = [
                {"path": "s3://bucket1/data", "folder": "data1"},
                {"path": "s3://bucket2/ref", "folder": "refs"},
            ]
            save_s3_mounts(temp_dir, mounts)

            runner = CliRunner()
            result = runner.invoke(s3_list, [temp_dir])

            assert result.exit_code == 0
            assert "data1" in result.output
            assert "refs" in result.output
            assert "s3://bucket1/data" in result.output


class TestS3Datasets:
    """Test suite for S3 dataset listing."""

    @patch("resgen.sync.folder.boto3")
    def test_get_s3_datasets_basic(self, mock_boto3):
        """Test basic S3 dataset listing."""
        # Mock S3 client and paginator
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        # Mock S3 response
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "data/file1.bw"},
                    {"Key": "data/file2.bw"},
                ]
            }
        ]

        datasets = get_s3_datasets("s3://test-bucket/data")

        # Should have folder + 2 files
        assert len(datasets) == 3

        # Check folder
        folder = next(d for d in datasets if d["is_folder"])
        assert folder["name"] == "data"
        assert folder["fullpath"] == "data"

        # Check files
        files = [d for d in datasets if not d["is_folder"]]
        assert len(files) == 2
        assert all(d["is_s3"] for d in files)
        assert all("s3_uri" in d for d in files)

    @patch("resgen.sync.folder.boto3")
    def test_get_s3_datasets_with_index_files(self, mock_boto3):
        """Test S3 dataset listing with index file association."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        # Mock S3 response with .bam and .bai
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "prefix/test.bam"},
                    {"Key": "prefix/test.bam.bai"},
                ]
            }
        ]

        datasets = get_s3_datasets("s3://test-bucket/prefix")

        # Should have folder + bam file (.bai should be removed)
        files = [d for d in datasets if not d["is_folder"]]
        assert len(files) == 1

        bam_file = files[0]
        assert bam_file["name"] == "test.bam"
        assert "index_filepath" in bam_file
        assert bam_file["index_filepath"] == "s3://test-bucket/prefix/test.bam.bai"

    @patch("resgen.sync.folder.boto3")
    def test_get_s3_datasets_with_folder_prefix(self, mock_boto3):
        """Test S3 dataset listing with folder prefix."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "data/file.bw"},
                ]
            }
        ]

        datasets = get_s3_datasets("s3://test-bucket/data", folder_prefix="refs")

        # Files should be under refs/data/
        file_dataset = next(d for d in datasets if not d["is_folder"] and d["name"] == "file.bw")
        assert file_dataset["fullpath"] == "refs/data/file.bw"

    def test_get_s3_datasets_invalid_path(self):
        """Test that invalid S3 path raises error."""
        with pytest.raises(ValueError, match="Invalid S3 path"):
            get_s3_datasets("not-an-s3-path")


class TestS3SyncIntegration:
    """Test suite for S3 sync integration."""

    @patch("resgen.manage.rg")
    @patch("resgen.manage._get_directory_url")
    @patch("resgen.manage.get_local_datasets")
    @patch("resgen.manage.get_s3_datasets")
    @patch("resgen.manage.load_s3_mounts")
    @patch("resgen.manage.get_remote_datasets")
    @patch("resgen.manage.add_and_update_local_datasets")
    @patch("resgen.manage.remove_stale_remote_datasets")
    @patch("resgen.manage.can_sync_datasets")
    def test_sync_with_s3_mounts(
        self,
        mock_can_sync,
        mock_remove_stale,
        mock_add_update,
        mock_get_remote,
        mock_load_mounts,
        mock_get_s3,
        mock_get_local,
        mock_get_url,
        mock_rg,
    ):
        """Test sync with S3 mounts merges local and S3 datasets."""
        directory = "/test/dir"
        mock_get_url.return_value = "http://localhost:1807"

        mock_connection = MagicMock()
        mock_project = MagicMock()
        mock_rg.connect.return_value = mock_connection
        mock_connection.find_or_create_project.return_value = mock_project

        # Local datasets
        local_datasets = [{"name": "local.bw", "fullpath": "local.bw", "is_folder": False}]
        mock_get_local.return_value = local_datasets

        # S3 mount
        mock_load_mounts.return_value = [
            {"path": "s3://bucket/data", "folder": "refs"}
        ]

        # S3 datasets
        s3_datasets = [
            {"name": "refs", "fullpath": "refs", "is_folder": True, "is_s3": True},
            {"name": "s3file.bw", "fullpath": "refs/s3file.bw", "is_folder": False, "is_s3": True, "s3_uri": "s3://bucket/data/s3file.bw"}
        ]
        mock_get_s3.return_value = s3_datasets

        mock_get_remote.return_value = {}

        _sync_datasets(directory)

        # Verify S3 datasets were loaded
        mock_get_s3.assert_called_once_with("s3://bucket/data", folder_prefix="refs")

        # Verify add_and_update was called with merged datasets (local + s3)
        call_args = mock_add_update.call_args[0]
        merged_datasets = call_args[1]
        assert len(merged_datasets) == 3  # 1 local + 2 S3 (folder + file)

    @patch("resgen.manage.rg")
    @patch("resgen.manage._get_directory_url")
    @patch("resgen.manage.get_s3_datasets")
    @patch("resgen.manage.get_remote_datasets")
    @patch("resgen.manage.add_and_update_local_datasets")
    @patch("resgen.manage.remove_stale_remote_datasets")
    @patch("resgen.manage.can_sync_datasets")
    def test_sync_direct_s3_path(
        self,
        mock_can_sync,
        mock_remove_stale,
        mock_add_update,
        mock_get_remote,
        mock_get_s3,
        mock_get_url,
        mock_rg,
    ):
        """Test sync directly from S3 path."""
        s3_path = "s3://test-bucket/genomics-data"
        mock_get_url.return_value = "http://localhost:1807"

        mock_connection = MagicMock()
        mock_project = MagicMock()
        mock_rg.connect.return_value = mock_connection
        mock_connection.find_or_create_project.return_value = mock_project

        # S3 datasets
        s3_datasets = [
            {"name": "file.bw", "fullpath": "file.bw", "is_folder": False, "is_s3": True, "s3_uri": "s3://test-bucket/genomics-data/file.bw"}
        ]
        mock_get_s3.return_value = s3_datasets
        mock_get_remote.return_value = {}

        _sync_datasets(s3_path)

        # Verify project name is derived from S3 path
        mock_connection.find_or_create_project.assert_called_once_with("genomics-data")

        # Verify S3 datasets were loaded
        mock_get_s3.assert_called_once_with(s3_path)

        # Verify sync was called with S3 datasets
        call_args = mock_add_update.call_args[0]
        datasets = call_args[1]
        assert len(datasets) == 1
        assert datasets[0]["is_s3"]