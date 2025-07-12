from tempfile import TemporaryDirectory
from unittest.mock import patch
from resgen.manage import _sync_datasets
from resgen.license import LicenseInfo, LicenseError
from os.path import join
import pytest

@patch("resgen.manage.rg")
@patch("resgen.manage.get_license", return_value=LicenseInfo(
    permissions='guest',
    username='guest'
))
def test_sync_datasets(_1,_2):
    with TemporaryDirectory() as tmpdir:
        open(join(tmpdir, 'blah1'), 'w').write('blah')

        # 1 dataset is allowed
        with patch("resgen.manage.datasets_allowed", return_value=1):
            _sync_datasets(tmpdir)

        open(join(tmpdir, 'blah2'), 'w').write('blah')
        
        # 2 datasets are not allowed
        with patch("resgen.manage.datasets_allowed", return_value=1):
            with pytest.raises(LicenseError):
                _sync_datasets(tmpdir)