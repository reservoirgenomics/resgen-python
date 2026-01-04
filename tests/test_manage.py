import os
import tempfile
from unittest.mock import MagicMock, patch, call
import pytest

from resgen.manage import _sync_datasets, can_sync_datasets
from resgen.license import LicenseInfo, LicenseError
from resgen.exceptions import ResgenError
from resgen import UnknownConnectionException


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