from resgen.license import get_license, LicenseInfo, license_info
from unittest.mock import patch
import os
import pytest
from os.path import join
from tempfile import TemporaryDirectory


class TestLicense:

    def test_get_license_2(self):
        """
        Test that get_license returns a guest license when no filepath is provided
        and no RESGEN_LICENSE_JWT environment variable is set.
        """
        with patch.dict(os.environ, {}, clear=True):
            license_info = get_license(None)
            assert isinstance(license_info, LicenseInfo)
            assert license_info.permissions == "guest"
            assert license_info.username == "guest"

    def test_get_license_3(self):
        """
        Test get_license when filepath is None and LICENSE_JWT is set in environment.

        This test verifies that when no filepath is provided and the RESGEN_LICENSE_JWT
        environment variable is set, the function correctly retrieves and returns
        the license information from the environment variable.
        """
        # Set up the environment variable
        test_jwt = "header.payload.signature"
        os.environ["RESGEN_LICENSE_JWT"] = test_jwt

        # Mock the license_info function
        def mock_license_info(jwt):
            return LicenseInfo(permissions="member", username="testuser")

        with patch("resgen.license.license_info", mock_license_info):
            try:
                # Call the function under test
                result = get_license(None)

                # Assert the result
                assert isinstance(result, LicenseInfo)
                assert result.permissions == "member"
                assert result.username == "testuser"
            finally:
                # Clean up: restore the original license_info function and remove the environment variable
                del os.environ["RESGEN_LICENSE_JWT"]

    def test_get_license_empty_environment_variable(self):
        """
        Test get_license when RESGEN_LICENSE_JWT environment variable is empty.
        This should return a guest license.
        """
        os.environ["RESGEN_LICENSE_JWT"] = ""
        result = get_license(None)
        assert isinstance(result, LicenseInfo)
        assert result.permissions == "guest"
        assert result.username == "guest"

    def test_get_license_nonexistent_file(self):
        """
        Test get_license with a non-existent file path.
        This should raise a FileNotFoundError.
        """
        with pytest.raises(FileNotFoundError):
            get_license("/path/to/nonexistent/file")

    def test_get_license_unset_environment_variable(self):
        """
        Test get_license when RESGEN_LICENSE_JWT environment variable is not set.
        This should return a guest license.
        """
        if "RESGEN_LICENSE_JWT" in os.environ:
            del os.environ["RESGEN_LICENSE_JWT"]
        result = get_license(None)
        assert isinstance(result, LicenseInfo)
        assert result.permissions == "guest"
        assert result.username == "guest"

    def test_get_license_with_filepath(self):
        """
        Test get_license function when a filepath is provided.
        This test verifies that the function correctly reads the license from the specified file
        and returns the appropriate LicenseInfo object.
        """
        # Create a temporary license file
        with TemporaryDirectory() as tmpdir:
            temp_license_file = join(tmpdir, "temp_license.txt")

            with open(temp_license_file, "w") as f:
                f.write("x123")

            with patch("resgen.license.license_info") as mock_license_info:
                # Mock the license_info function to return a LicenseInfo object
                get_license(temp_license_file)
                mock_license_info.assert_called_with("x123")
